"""
This module implements the MQTT transport for the MCP server.
"""
from contextlib import AsyncExitStack
import json
from uuid import uuid4
from datetime import timedelta
import random
from pydantic import AnyUrl, BaseModel
from mcp.client.session import ClientSession, SamplingFnT, ListRootsFnT, LoggingFnT, MessageHandlerFnT
from mcp.shared.exceptions import McpError
from mcp.shared.mqtt import MqttTransportBase, MqttOptions, QOS, MCP_SERVER_NAME_FILTERS
import asyncio
import anyio.to_thread as anyio_to_thread
import anyio.from_thread as anyio_from_thread
import traceback
import mcp.shared.mqtt_topic as mqtt_topic
import paho.mqtt.client as mqtt
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions
import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from typing import Any, Literal, TypeAlias, Callable, Awaitable
import mcp.types as types

RcvStream : TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage]
SndStream : TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage]
RcvStreamEx : TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
SndStreamEX : TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage | Exception]
ServerRun : TypeAlias = Callable[[RcvStreamEx, SndStream], Awaitable[Any]]

ServerName : TypeAlias = str
ServerId : TypeAlias = str
InitializeResult : TypeAlias = Literal["ok"] | Literal["already_connected"] | tuple[Literal["error"], str]
ConnectResult : TypeAlias = tuple[Literal["ok"], types.InitializeResult] | tuple[Literal["error"], Any]

logger = logging.getLogger(__name__)

class ServerDefinition(BaseModel):
    description: str
    meta: dict[str, Any] = {}

class ServerOnlineNotification(BaseModel):
    jsonrpc: Literal["2.0"]
    method: str = "notifications/server/online"
    params: ServerDefinition

class MqttClientSession(ClientSession):
    def __init__(
        self,
        server_id: ServerId,
        server_name: ServerName,
        read_stream: MemoryObjectReceiveStream[types.JSONRPCMessage | Exception],
        write_stream: MemoryObjectSendStream[types.JSONRPCMessage],
        read_timeout_seconds: timedelta | None = None,
        sampling_callback: SamplingFnT | None = None,
        list_roots_callback: ListRootsFnT | None = None,
        logging_callback: LoggingFnT | None = None,
        message_handler: MessageHandlerFnT | None = None,
    ) -> None:
        super().__init__(
            read_stream,
            write_stream,
            read_timeout_seconds,
            sampling_callback,
            list_roots_callback,
            logging_callback,
            message_handler,
        )
        self.server_id = server_id
        self.server_name = server_name
        self.server_info: types.InitializeResult | None = None

class MqttTransportClient(MqttTransportBase):

    def __init__(self,
                 mcp_client_name: str,
                 client_id: str | None = None,
                 server_name_filter: str | list[str] = '#',
                 auto_connect_to_mcp_server: bool = False,
                 on_mcp_connect: Callable[["MqttTransportClient", ServerName, ConnectResult], Awaitable[Any]] | None = None,
                 on_mcp_disconnect: Callable[["MqttTransportClient", ServerName], Awaitable[Any]] | None = None,
                 on_mcp_server_discovered: Callable[["MqttTransportClient", ServerName], Awaitable[Any]] | None = None,
                 mqtt_options: MqttOptions = MqttOptions()):
        uuid = uuid4().hex
        mqtt_clientid = client_id if client_id else uuid
        self._current_server_id: dict[ServerName, ServerId] = {}
        self.server_list: dict[ServerName, dict[ServerId, ServerDefinition]] = {}
        self.client_sessions: dict[ServerName, MqttClientSession] = {}
        self.mcp_client_id = mqtt_clientid
        self.mcp_client_name = mcp_client_name
        if isinstance(server_name_filter, str):
            self.server_name_filter = [server_name_filter]
        else:
            self.server_name_filter = server_name_filter
        self.auto_connect_to_mcp_server = auto_connect_to_mcp_server
        self.on_mcp_connect = on_mcp_connect
        self.on_mcp_disconnect = on_mcp_disconnect
        self.on_mcp_server_discovered = on_mcp_server_discovered
        self.client_capability_change_topic = mqtt_topic.get_client_capability_change_topic(self.mcp_client_id)
        ## Send disconnected notification when disconnects
        self._disconnected_msg = types.JSONRPCMessage(
            types.JSONRPCNotification(
                jsonrpc="2.0",
                method = "notifications/disconnected"
            )
        )
        super().__init__("mcp-client", mqtt_clientid = mqtt_clientid,
                         mqtt_options = mqtt_options,
                         disconnected_msg = self._disconnected_msg,
                         disconnected_msg_retain = False)

    def get_presence_topic(self) -> str:
        return mqtt_topic.get_client_presence_topic(self.mcp_client_id)

    async def start(self, timeout: timedelta | None = None) -> bool | str:
        try:
            connect_result = self.connect()
            asyncio.create_task(anyio_to_thread.run_sync(self.client.loop_forever))
            if connect_result != mqtt.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to connect to MQTT broker, error code: {connect_result}")
                return mqtt.error_string(connect_result)
            # test if the client is connected and wait until it is connected
            if timeout:
                while not self.is_connected():
                    await asyncio.sleep(0.1)
                    if timeout.total_seconds() <= 0:
                        last_fail_reason = self.get_last_connect_fail_reason()
                        if last_fail_reason:
                            return last_fail_reason.getName()
                        return "timeout"
                    timeout -= timedelta(seconds=0.1)
            return True
        except asyncio.CancelledError:
            logger.debug("MQTT transport (MCP client) got cancelled")
            return "cancelled"
        except ConnectionRefusedError as exc:
            logger.error(f"MQTT transport (MCP client) failed to connect: {exc}")
            return "connection_refused"
        except TimeoutError as exc:
            logger.error(f"MQTT transport (MCP client) timed out: {exc}")
            return "timeout"
        except Exception as exc:
            logger.error(f"MQTT transport (MCP client) failed: {exc}")
            return f"connect mqtt error: {str(exc)}"

    def get_session(self, server_name: ServerName) -> MqttClientSession | None:
        return self.client_sessions.get(server_name, None)

    async def initialize_mcp_server(
            self, server_name: str,
            read_timeout_seconds: timedelta | None = None,
            sampling_callback: SamplingFnT | None = None,
            list_roots_callback: ListRootsFnT | None = None,
            logging_callback: LoggingFnT | None = None,
            message_handler: MessageHandlerFnT | None = None) -> InitializeResult:
        if server_name in self.client_sessions:
            return "already_connected"
        if server_name not in self.server_list:
            logger.error(f"MCP server not found, server name: {server_name}")
            return ("error", "MCP server not found")
        server_id = self.pick_server_id(server_name)

        async def after_subscribed(
                subscribe_result: Literal["success", "error"]
            ):
            if subscribe_result == "error":
                if self.on_mcp_connect:
                    self._task_group.start_soon(self.on_mcp_connect, self, server_name, ("error", "subscribe_mcp_server_topics_failed"))
            client_session = self._create_session(
                server_id,
                server_name,
                read_timeout_seconds,
                sampling_callback,
                list_roots_callback,
                logging_callback,
                message_handler
            )
            self.client_sessions[server_name] = client_session
            try:
                logger.debug(f"before initialize: {server_name}")
                async def after_initialize():
                    exit_stack = AsyncExitStack()
                    try:
                        session = await exit_stack.enter_async_context(client_session)
                        init_result = await session.initialize()
                        session.server_info = init_result
                        if self.on_mcp_connect:
                            self._task_group.start_soon(self.on_mcp_connect, self, server_name, ("ok", init_result))
                    except Exception as e:
                        self.client_sessions.pop(server_name)
                        logging.error(f"Failed to initialize server {server_name}: {e}")
                        await exit_stack.aclose()
                self._task_group.start_soon(after_initialize)
                logger.debug(f"after initialize: {server_name}")
            except McpError as exc:
                self.client_sessions.pop(server_name)
                logger.error(f"Failed to connect to MCP server: {exc}")
                if self.on_mcp_connect:
                    self._task_group.start_soon(self.on_mcp_connect, self, server_name, ("error", McpError))

        if self._subscribe_mcp_server_topics(server_id, server_name, after_subscribed):
            return "ok"
        else:
            return ("error", "send_subscribe_request_failed")

    async def deinitialize_mcp_server(self, server_name: ServerName) -> None:
        server_id = self._current_server_id[server_name]
        topic = mqtt_topic.get_rpc_topic(self.mcp_client_id, server_id, server_name)
        self.publish_json_rpc_message(topic, message = self._disconnected_msg, retain=False)
        self._remove_server(server_id, server_name)

    async def send_ping(self, server_name: ServerName) -> bool | types.EmptyResult:
        return await self._with_session(server_name, lambda s: s.send_ping())

    async def send_progress_notification(
        self, server_name: ServerName, progress_token: str | int,
        progress: float, total: float | None = None
    ) -> bool | None:
        return await self._with_session(server_name, lambda s: s.send_progress_notification(progress_token, progress, total))

    async def set_logging_level(self, server_name: ServerName,
                                level: types.LoggingLevel) -> bool | types.EmptyResult:
        return await self._with_session(server_name, lambda s: s.set_logging_level(level))

    async def list_resources(self, server_name: ServerName) -> bool | types.ListResourcesResult:
        return await self._with_session(server_name, lambda s: s.list_resources())

    async def list_resource_templates(self, server_name: ServerName) -> bool | types.ListResourceTemplatesResult:
        return await self._with_session(server_name, lambda s: s.list_resource_templates())

    async def read_resource(self, server_name: ServerName,
                            uri: AnyUrl) -> bool | types.ReadResourceResult:
        return await self._with_session(server_name, lambda s: s.read_resource(uri))

    async def subscribe_resource(self, server_name: ServerName,
                                 uri: AnyUrl) -> bool | types.EmptyResult:
        return await self._with_session(server_name, lambda s: s.subscribe_resource(uri))

    async def unsubscribe_resource(self, server_name: ServerName,
                                   uri: AnyUrl) -> bool | types.EmptyResult:
        return await self._with_session(server_name, lambda s: s.unsubscribe_resource(uri))

    async def call_tool(
        self, server_name: ServerName, name: str, arguments: dict[str, Any] | None = None
    ) -> bool | types.CallToolResult:
        return await self._with_session(server_name, lambda s: s.call_tool(name, arguments))

    async def list_prompts(self, server_name: ServerName) -> bool | types.ListPromptsResult:
        return await self._with_session(server_name, lambda s: s.list_prompts())

    async def get_prompt(
        self, server_name: ServerName, name: str, arguments: dict[str, str] | None = None
    ) -> bool | types.GetPromptResult:
        return await self._with_session(server_name, lambda s: s.get_prompt(name, arguments))

    async def complete(
        self,
        server_name: ServerName,
        ref: types.ResourceReference | types.PromptReference,
        argument: dict[str, str],
    ) -> bool | types.CompleteResult:
        return await self._with_session(server_name, lambda s: s.complete(ref, argument))

    async def list_tools(self, server_name: ServerName) -> bool | types.ListToolsResult:
        return await self._with_session(server_name, lambda s: s.list_tools())

    async def send_roots_list_changed(self, server_name: ServerName) -> bool | None:
        return await self._with_session(server_name, lambda s: s.send_roots_list_changed())

    async def _with_session(
            self, server_name: ServerName,
            async_callback: Callable[[MqttClientSession], Awaitable[bool | Any]]) -> bool | Any:
        if not (client_session := self.client_sessions.get(server_name, None)):
            logger.error(f"No session for server_name: {server_name}")
            return False
        return await async_callback(client_session)

    def _create_session(
            self, server_id: ServerId, server_name: ServerName,
            read_timeout_seconds: timedelta | None = None,
            sampling_callback: SamplingFnT | None = None,
            list_roots_callback: ListRootsFnT | None = None,
            logging_callback: LoggingFnT | None = None,
            message_handler: MessageHandlerFnT | None = None
    ):
        ## Streams are used to communicate between the MqttTransportClient and the MCPSession:
        ## 1. (msg) --> MqttBroker --> MqttTransportClient -->[read_stream_writer]-->[read_stream]--> MCPSession
        ## 2. MqttBroker <-- MqttTransportClient <--[write_stream_reader]--[write_stream]-- MCPSession <-- (msg)
        read_stream: RcvStreamEx
        read_stream_writer: SndStreamEX
        write_stream: SndStream
        write_stream_reader: RcvStream
        read_stream_writer, read_stream = anyio.create_memory_object_stream(0) # type: ignore
        write_stream, write_stream_reader = anyio.create_memory_object_stream(0) # type: ignore
        self._read_stream_writers[server_id] = read_stream_writer
        self._task_group.start_soon(self._receieved_from_session, server_id, server_name, write_stream_reader)
        logger.debug(f"Created new session for server_id: {server_id}")
        return MqttClientSession(
            server_id,
            server_name,
            read_stream,
            write_stream,
            read_timeout_seconds,
            sampling_callback,
            list_roots_callback,
            logging_callback,
            message_handler
        )

    def _on_connect(self, client: mqtt.Client, userdata: Any, connect_flags: mqtt.ConnectFlags, reason_code : ReasonCode, properties: Properties | None):
        super()._on_connect(client, userdata, connect_flags, reason_code, properties)
        if properties and hasattr(properties, "UserProperty"):
            user_properties: dict[str, Any] = dict(properties.UserProperty) # type: ignore
            if MCP_SERVER_NAME_FILTERS in user_properties:
                self.server_name_filter = json.loads(user_properties[MCP_SERVER_NAME_FILTERS])
                logger.debug(f"Use broker suggested server name filters: {self.server_name_filter}")
        if reason_code == 0:
            ## Subscribe to the MCP server's presence topic
            for snf in self.server_name_filter:
                logger.debug(f"Subscribing to server presence topic for server_name_filter: {snf}")
                client.subscribe(mqtt_topic.get_server_presence_topic('+', snf), qos=QOS)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        logger.debug(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        match msg.topic:
            case str() as t if t.startswith(mqtt_topic.SERVER_PRESENCE_BASE):
                self._handle_server_presence_message(msg)
            case str() as t if t.startswith(mqtt_topic.RPC_BASE):
                self._handle_rpc_message(msg)
            case str() as t if t.startswith(mqtt_topic.SERVER_CAPABILITY_CHANGE_BASE):
                self._handle_server_capability_message(msg)
            case _:
                logger.error(f"Received message on unexpected topic: {msg.topic}")

    def _on_subscribe(self, client: mqtt.Client, userdata: Any, mid: int,
                      reason_code_list: list[ReasonCode], properties: Properties | None):
        if mid in userdata.get("pending_subs", {}):
            server_name, server_id, after_subscribed = userdata["pending_subs"].pop(mid)
            ## only create session if all topic subscribed successfully
            if all([rc.value == QOS for rc in reason_code_list]):
                logger.debug(f"Subscribed to topics for server_name: {server_name}, server_id: {server_id}")
                anyio_from_thread.run(after_subscribed, "success")
            else:
                anyio_from_thread.run(after_subscribed, "error")
                logger.error(f"Failed to subscribe to topics for server_name: {server_name}, server_id: {server_id}, reason_codes: {reason_code_list}")

    def _handle_server_presence_message(self, msg: mqtt.MQTTMessage) -> None:
        topic_words = msg.topic.split("/")
        server_id = topic_words[2]
        server_name = "/".join(topic_words[3:])
        if msg.payload:
            newly_added_server = False if server_name in self.server_list else True
            server_notif = ServerOnlineNotification.model_validate_json(msg.payload.decode())
            self.server_list.setdefault(server_name, {})[server_id] = server_notif.params
            logger.debug(f"Server {server_name} with id {server_id} is online")
            if newly_added_server:
                if self.auto_connect_to_mcp_server:
                    logger.debug(f"Auto connecting to MCP server {server_name}")
                    anyio_from_thread.run(self.initialize_mcp_server, server_name)
                if self.on_mcp_server_discovered:
                    anyio_from_thread.run(self.on_mcp_server_discovered, self, server_name)
        else:
            # server is offline if the payload is empty
            logger.debug(f"Server {server_name} with id {server_id} is offline")
            self._remove_server(server_id, server_name)

    def _remove_server(self, server_id: ServerId, server_name: ServerName) -> None:
        if server_id in self.server_list.get(server_name, {}):
            self._read_stream_writers[server_id].close()

    def _handle_rpc_message(self, msg: mqtt.MQTTMessage) -> None:
        server_name = "/".join(msg.topic.split("/")[3:])
        anyio_from_thread.run(self._send_message_to_session, server_name, msg)

    def _handle_server_capability_message(self, msg: mqtt.MQTTMessage) -> None:
        server_name = "/".join(msg.topic.split("/")[4:])
        anyio_from_thread.run(self._send_message_to_session, server_name, msg)

    def _subscribe_mcp_server_topics(self, server_id: ServerId, server_name: ServerName,
                             after_subscribed: Callable[[Any], Awaitable[None]]):
        topic_filters = [
            (mqtt_topic.get_server_capability_change_topic(server_id, server_name), SubscribeOptions(qos=QOS)),
            (mqtt_topic.get_rpc_topic(self.mcp_client_id, server_id, server_name), SubscribeOptions(qos=QOS, noLocal=True))
        ]
        ret, mid = self.client.subscribe(topic=topic_filters)
        if ret != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to subscribe to topics for server_name: {server_name}")
            return False
        userdata = self.client.user_data_get()
        pending_subs = userdata.get("pending_subs", {})
        pending_subs[mid] = (server_name, server_id, after_subscribed)
        userdata["pending_subs"] = pending_subs
        return True

    async def _send_message_to_session(self, server_name: ServerName, msg: mqtt.MQTTMessage):
        if not (client_session := self.client_sessions.get(server_name, None)):
            logger.error(f"_send_message_to_session: No session for server_name: {server_name}")
            return
        payload = msg.payload.decode()
        server_id = client_session.server_id
        if server_id not in self._read_stream_writers:
            logger.error(f"No session for server_id: {server_id}")
            return
        read_stream_writer = self._read_stream_writers[server_id]
        try:
            message = types.JSONRPCMessage.model_validate_json(payload)
            logger.debug(f"Sending msg to session for server_id: {server_id}, msg: {message}")
            with anyio.fail_after(3):
                await read_stream_writer.send(message)
        except Exception as exc:
            logger.error(f"Failed to send msg to session for server_id: {server_id}, exception: {exc}")
            traceback.print_exc()
            ## TODO: the session does not handle exceptions for now
            #await read_stream_writer.send(exc)
    async def _receieved_from_session(self, server_id: ServerId, server_name: ServerName,
                                     write_stream_reader: RcvStream):
        async with write_stream_reader:
            async for msg in write_stream_reader:
                logger.debug(f"Got msg from session for server_id: {server_id}, msg: {msg}")
                match msg.model_dump():
                    case {"method": method} if method == "notifications/initialized":
                        logger.debug(f"Session initialized for server_id: {server_id}")
                        topic = mqtt_topic.get_rpc_topic(self.mcp_client_id, server_id, server_name)
                    case {"method": method} if method.endswith("/list_changed"):
                        topic = None
                        logger.warning("Resource updates should not be sent from the session. Ignoring.")
                    case {"method": method} if method == "initialize":
                        topic = mqtt_topic.get_server_control_topic(server_id, server_name)
                    case _:
                        topic = mqtt_topic.get_rpc_topic(self.mcp_client_id, server_id, server_name)
                if topic:
                    self.publish_json_rpc_message(topic, message = msg)
        # cleanup
        if server_id in self._read_stream_writers:
            logger.debug(f"Removing session for server_id: {server_id}")
            stream = self._read_stream_writers.pop(server_id)
            await stream.aclose()

        # unsubscribe from the topics
        logger.debug(f"Unsubscribing from topics for server_id: {server_id}, server_name: {server_name}")
        topic_filters = [
            mqtt_topic.get_server_capability_change_topic(server_id, server_name),
            mqtt_topic.get_rpc_topic(self.mcp_client_id, server_id, server_name)
        ]
        self.client.unsubscribe(topic=topic_filters)

        if server_id in self.server_list.get(server_name, {}):
            _ = self.server_list[server_name].pop(server_id)
            if not self.server_list[server_name]:
                _ = self.server_list.pop(server_name)
                if self.on_mcp_disconnect:
                    self._task_group.start_soon(self.on_mcp_disconnect, self, server_name)

        if server_name in self.client_sessions:
            _ = self.client_sessions.pop(server_name)

        if server_name in self._current_server_id:
            _ = self._current_server_id.pop(server_name)
        logger.debug(f"Session stream closed for server_id: {server_id}")

    def pick_server_id(self, server_name: str) -> ServerId:
        server_id = random.choice(list(self.server_list[server_name].keys()))
        self._current_server_id[server_name] = server_id
        return server_id

def validate_server_name(name: str):
    if "/" not in name:
        raise ValueError(f"Invalid server name: {name}, must contain a '/'")
    elif ("+" in name) or ("#" in name):
        raise ValueError(f"Invalid server name: {name}, must not contain '+' or '#'")
    elif name[0] == "/":
        raise ValueError(f"Invalid server name: {name}, must not start with '/'")
