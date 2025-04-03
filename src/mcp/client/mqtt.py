"""
This module implements the MQTT transport for the MCP server.
"""
from contextlib import AsyncExitStack
from uuid import uuid4
from datetime import timedelta
import random
from pydantic import AnyUrl, BaseModel
from mcp.client.session import ClientSession, SamplingFnT, ListRootsFnT, LoggingFnT, MessageHandlerFnT
from mcp.shared.exceptions import McpError
from mcp.shared.mqtt import MqttTransportBase, MqttOptions, QOS
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

ServiceName : TypeAlias = str
ServiceId : TypeAlias = str
InitializeResult : TypeAlias = Literal["ok"] | Literal["already_connected"] | tuple[Literal["error"], str]
ConnectResult : TypeAlias = tuple[Literal["ok"], types.InitializeResult] | tuple[Literal["error"], Any]
DisconnectReason : TypeAlias = Literal["client_initiated_disconnect", "server_initiated_disconnect"]

logger = logging.getLogger(__name__)

class ServiceDefinition(BaseModel):
    description: str
    meta: dict[str, Any] = {}

class ServiceOnlineNotification(BaseModel):
    jsonrpc: Literal["2.0"]
    method: str = "notifications/service/online"
    params: ServiceDefinition

class MqttClientSession(ClientSession):
    def __init__(
        self,
        service_id: ServiceId,
        service_name: ServiceName,
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
        self.service_id = service_id
        self.service_name = service_name
        self.server_info: types.InitializeResult | None = None

class MqttTransportClient(MqttTransportBase):

    def __init__(self, mcp_client_name: str, client_id_prefix: str | None = None,
                 service_name_filter: str = '#',
                 auto_connect_to_mcp_server: bool = False,
                 on_mcp_connect: Callable[["MqttTransportClient", ServiceName, ConnectResult], Awaitable[Any]] | None = None,
                 on_mcp_disconnect: Callable[["MqttTransportClient", ServiceName, DisconnectReason], Awaitable[Any]] | None = None,
                 on_mcp_server_presence: Callable[["MqttTransportClient", ServiceName, Literal["online", "offline"]], Awaitable[Any]] | None = None,
                 mqtt_options: MqttOptions = MqttOptions()):
        self.exit_stack: AsyncExitStack = AsyncExitStack()
        uuid = uuid4().hex
        mqtt_clientid = f"{client_id_prefix}-{uuid}" if client_id_prefix else uuid
        self.service_list: dict[ServiceName, dict[ServiceId, ServiceDefinition]] = {}
        self.service_sessions: dict[ServiceName, MqttClientSession] = {}
        self.mcp_client_id = mqtt_clientid
        self.mcp_client_name = mcp_client_name
        self.service_name_filter = service_name_filter
        self.auto_connect_to_mcp_server = auto_connect_to_mcp_server #TODO: not implemented yet
        self.on_mcp_connect = on_mcp_connect
        self.on_mcp_disconnect = on_mcp_disconnect #TODO: not implemented yet
        self.on_mcp_server_presence = on_mcp_server_presence
        self.client_capability_change_topic = mqtt_topic.get_client_capability_change_topic(self.mcp_client_id)
        super().__init__("mcp-client", mqtt_clientid = mqtt_clientid, mqtt_options = mqtt_options)
        self.presence_topic = mqtt_topic.get_client_presence_topic(self.mcp_client_id)
        ## Send disconnected notification when disconnects
        self.disconnected_msg = types.JSONRPCNotification(
            jsonrpc="2.0",
            method = "notifications/disconnected"
        )
        self.disconnected_msg_retain = False
        self.client.will_set(
            topic=self.presence_topic,
            payload=self.disconnected_msg.model_dump_json(),
            qos=QOS
        )

    def start(self):
        def do_start():
            self.connect()
            self.client.loop_forever()
        try:
            asyncio.create_task(anyio_to_thread.run_sync(do_start))
        except asyncio.CancelledError:
            logger.debug("MQTT transport (MCP client) got cancelled")
        except Exception as exc:
            logger.error(f"MQTT transport (MCP client) failed: {exc}")
            traceback.print_exc()

    async def initialize_mcp_server(
            self, service_name: str,
            read_timeout_seconds: timedelta | None = None,
            sampling_callback: SamplingFnT | None = None,
            list_roots_callback: ListRootsFnT | None = None,
            logging_callback: LoggingFnT | None = None,
            message_handler: MessageHandlerFnT | None = None) -> InitializeResult:
        if service_name in self.service_sessions:
            return "already_connected"
        if service_name not in self.service_list:
            logger.error(f"MCP server not found, service name: {service_name}")
            return ("error", "MCP server not found")
        service_id = self.pick_service_id(service_name)

        async def after_subscribed(
                subscribe_result: Literal["success", "error"]
            ):
            if subscribe_result == "error":
                if self.on_mcp_connect:
                    self._task_group.start_soon(self.on_mcp_connect, self, service_name, ("error", "subscribe_mcp_server_topics_failed"))
            client_session = self._create_session(
                service_id,
                service_name,
                read_timeout_seconds,
                sampling_callback,
                list_roots_callback,
                logging_callback,
                message_handler
            )
            self.service_sessions[service_name] = client_session
            try:
                logger.debug(f"before initialize: {service_name}")
                async def after_initialize():
                    try:
                        session = await self.exit_stack.enter_async_context(client_session)
                        init_result = await session.initialize()
                        session.server_info = init_result
                        if self.on_mcp_connect:
                            self._task_group.start_soon(self.on_mcp_connect, self, service_name, ("ok", init_result))
                    except Exception as e:
                        logging.error(f"Failed to initialize server {service_name}: {e}")
                        await self.exit_stack.aclose()
                        raise
                self._task_group.start_soon(after_initialize)
                logger.debug(f"after initialize: {service_name}")
            except McpError as exc:
                logger.error(f"Failed to connect to MCP server: {exc}")
                if self.on_mcp_connect:
                    self._task_group.start_soon(self.on_mcp_connect, self, service_name, ("error", McpError))

        if self._subscribe_mcp_server_topics(service_id, service_name, after_subscribed):
            return "ok"
        else:
            return ("error", "send_subscribe_request_failed")

    async def send_ping(self, service_name: ServiceName) -> bool | types.EmptyResult:
        return await self._with_session(service_name, lambda s: s.send_ping())

    async def send_progress_notification(
        self, service_name: ServiceName, progress_token: str | int,
        progress: float, total: float | None = None
    ) -> bool | None:
        return await self._with_session(service_name, lambda s: s.send_progress_notification(progress_token, progress, total))

    async def set_logging_level(self, service_name: ServiceName,
                                level: types.LoggingLevel) -> bool | types.EmptyResult:
        return await self._with_session(service_name, lambda s: s.set_logging_level(level))

    async def list_resources(self, service_name: ServiceName) -> bool | types.ListResourcesResult:
        return await self._with_session(service_name, lambda s: s.list_resources())

    async def list_resource_templates(self, service_name: ServiceName) -> bool | types.ListResourceTemplatesResult:
        return await self._with_session(service_name, lambda s: s.list_resource_templates())

    async def read_resource(self, service_name: ServiceName,
                            uri: AnyUrl) -> bool | types.ReadResourceResult:
        return await self._with_session(service_name, lambda s: s.read_resource(uri))

    async def subscribe_resource(self, service_name: ServiceName,
                                 uri: AnyUrl) -> bool | types.EmptyResult:
        return await self._with_session(service_name, lambda s: s.subscribe_resource(uri))

    async def unsubscribe_resource(self, service_name: ServiceName,
                                   uri: AnyUrl) -> bool | types.EmptyResult:
        return await self._with_session(service_name, lambda s: s.unsubscribe_resource(uri))

    async def call_tool(
        self, service_name: ServiceName, name: str, arguments: dict[str, Any] | None = None
    ) -> bool | types.CallToolResult:
        return await self._with_session(service_name, lambda s: s.call_tool(name, arguments))

    async def list_prompts(self, service_name: ServiceName) -> bool | types.ListPromptsResult:
        return await self._with_session(service_name, lambda s: s.list_prompts())

    async def get_prompt(
        self, service_name: ServiceName, name: str, arguments: dict[str, str] | None = None
    ) -> bool | types.GetPromptResult:
        return await self._with_session(service_name, lambda s: s.get_prompt(name, arguments))

    async def complete(
        self,
        service_name: ServiceName,
        ref: types.ResourceReference | types.PromptReference,
        argument: dict[str, str],
    ) -> bool | types.CompleteResult:
        return await self._with_session(service_name, lambda s: s.complete(ref, argument))

    async def list_tools(self, service_name: ServiceName) -> bool | types.ListToolsResult:
        return await self._with_session(service_name, lambda s: s.list_tools())

    async def send_roots_list_changed(self, service_name: ServiceName) -> bool | None:
        return await self._with_session(service_name, lambda s: s.send_roots_list_changed())

    async def _with_session(
            self, service_name: ServiceName,
            async_callback: Callable[[MqttClientSession], Awaitable[bool | Any]]) -> bool | Any:
        if not (client_session := self.service_sessions.get(service_name)):
            logger.error(f"No session for service_name: {service_name}")
            return False
        return await async_callback(client_session)

    def _create_session(
            self, service_id: ServiceId, service_name: ServiceName,
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
        read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
        write_stream, write_stream_reader = anyio.create_memory_object_stream(0)
        self._read_stream_writers[service_id] = read_stream_writer
        self._task_group.start_soon(self._receieved_from_session, service_id, service_name, write_stream_reader)
        logger.debug(f"Created new session for service_id: {service_id}")
        return MqttClientSession(
            service_id,
            service_name,
            read_stream,
            write_stream,
            read_timeout_seconds,
            sampling_callback,
            list_roots_callback,
            logging_callback,
            message_handler
        )

    def _on_connect(self, client: mqtt.Client, userdata: Any, connect_flags: mqtt.ConnectFlags, reason_code : ReasonCode, properties: Properties | None):
        if reason_code == 0:
            super()._on_connect(client, userdata, connect_flags, reason_code, properties)
            ## Subscribe to the MCP server's service presence topic
            client.subscribe(mqtt_topic.get_service_presence_topic('+', self.service_name_filter), qos=QOS)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        logger.debug(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        match msg.topic:
            case str() as t if t.startswith(mqtt_topic.SERVICE_PRESENCE_BASE):
                self._handle_service_presence_message(msg)
            case str() as t if t.startswith(mqtt_topic.RPC_BASE):
                self._handle_rpc_message(msg)
            case str() as t if t.startswith(mqtt_topic.SERVICE_CAPABILITY_CHANGE_BASE):
                self._handle_service_capability_list_changed_message(msg)
            case str() as t if t.startswith(mqtt_topic.SERVICE_RESOURCE_UPDATE_BASE):
                self._handle_service_capability_resource_updated_message(msg)
            case _:
                logger.error(f"Received message on unexpected topic: {msg.topic}")

    def _on_subscribe(self, client: mqtt.Client, userdata: Any, mid: int,
                      reason_code_list: list[ReasonCode], properties: Properties | None):
        if mid in userdata.get("pending_subs", {}):
            service_name, service_id, after_subscribed = userdata["pending_subs"].pop(mid)
            ## only create session if all topic subscribed successfully
            if all([rc.value == QOS for rc in reason_code_list]):
                logger.debug(f"Subscribed to topics for service_name: {service_name}, service_id: {service_id}")
                anyio_from_thread.run(after_subscribed, "success")
            else:
                anyio_from_thread.run(after_subscribed, "error")
                logger.error(f"Failed to subscribe to topics for service_name: {service_name}, service_id: {service_id}, reason_codes: {reason_code_list}")

    def _handle_service_presence_message(self, msg: mqtt.MQTTMessage) -> None:
        topic_words = msg.topic.split("/")
        service_id = topic_words[2]
        service_name = "/".join(topic_words[3:])
        if msg.payload:
            newly_added_service = False if service_name in self.service_list else True
            service_notif = ServiceOnlineNotification.model_validate_json(msg.payload.decode())
            self.service_list.setdefault(service_name, {})[service_id] = service_notif.params
            logger.debug(f"Service {service_name} with id {service_id} is online")
            if newly_added_service:
                if self.on_mcp_server_presence:
                    anyio_from_thread.run(self.on_mcp_server_presence, self, service_name, "online")
        else:
            existing_service = True if service_name in self.service_list else False
            if service_id in self.service_list.get(service_name, {}):
                logger.debug(f"Service {service_name} with id {service_id} is offline")
                self.service_list[service_name].pop(service_id)
            if existing_service:
                if self.on_mcp_server_presence:
                    anyio_from_thread.run(self.on_mcp_server_presence, self, service_name, "offline")

    def _handle_rpc_message(self, msg: mqtt.MQTTMessage) -> None:
        service_name = "/".join(msg.topic.split("/")[2:])
        anyio_from_thread.run(self._send_message_to_session, service_name, msg)

    def _handle_service_capability_list_changed_message(self, msg: mqtt.MQTTMessage) -> None:
        service_name = "/".join(msg.topic.split("/")[4:])
        anyio_from_thread.run(self._send_message_to_session, service_name, msg)

    def _handle_service_capability_resource_updated_message(self, msg: mqtt.MQTTMessage) -> None:
        service_name = "/".join(msg.topic.split("/")[4:])
        anyio_from_thread.run(self._send_message_to_session, service_name, msg)

    def _subscribe_mcp_server_topics(self, service_id: ServiceId, service_name: ServiceName,
                             after_subscribed: Callable[[Any], Awaitable[None]]):
        topic_filters = [
            (mqtt_topic.get_service_capability_change_topic(service_id, service_name), SubscribeOptions(qos=QOS)),
            (mqtt_topic.get_service_resource_update_topic(service_id, service_name), SubscribeOptions(qos=QOS)),
            (mqtt_topic.get_rpc_topic(self.mcp_client_id, service_name), SubscribeOptions(qos=QOS, noLocal=True))
        ]
        ret, mid = self.client.subscribe(topic=topic_filters)
        if ret != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to subscribe to topics for service_name: {service_name}")
            return False
        userdata = self.client.user_data_get()
        pending_subs = userdata.get("pending_subs", {})
        pending_subs[mid] = (service_name, service_id, after_subscribed)
        userdata["pending_subs"] = pending_subs
        return True

    async def _send_message_to_session(self, service_name: ServiceName, msg: mqtt.MQTTMessage):
        if service_name not in self.service_sessions:
            logger.error(f"_send_message_to_session: No session for service_name: {service_name}")
            return
        client_session: MqttClientSession = self.service_sessions[service_name]
        payload = msg.payload.decode()
        service_id = client_session.service_id
        if service_id not in self._read_stream_writers:
            logger.error(f"No session for service_id: {service_id}")
            return
        read_stream_writer = self._read_stream_writers[service_id]
        try:
            message = types.JSONRPCMessage.model_validate_json(payload)
            logger.debug(f"Sending msg to session for service_id: {service_id}, msg: {message}")
            with anyio.fail_after(3):
                await read_stream_writer.send(message)
        except Exception as exc:
            logger.error(f"Failed to send msg to session for service_id: {service_id}, exception: {exc}")
            traceback.print_exc()
            ## TODO: the session does not handle exceptions for now
            #await read_stream_writer.send(exc)
    async def _receieved_from_session(self, service_id: ServiceId, service_name: ServiceName,
                                     write_stream_reader: RcvStream):
        async with write_stream_reader:
            async for msg in write_stream_reader:
                logger.debug(f"Got msg from session for service_id: {service_id}, msg: {msg}")
                match msg.model_dump():
                    case {"method": method} if method == "notifications/initialized":
                        logger.debug(f"Session initialized for service_id: {service_id}")
                        topic = mqtt_topic.get_rpc_topic(self.mcp_client_id, service_name)
                    case {"method": method} if method.endswith("/list_changed"):
                        topic = None
                        logger.warning("Resource updates should not be sent from the session. Ignoring.")
                    case {"method": method} if method == "initialize":
                        topic = mqtt_topic.get_service_control_topic(service_name)
                    case _:
                        topic = mqtt_topic.get_rpc_topic(self.mcp_client_id, service_name)
                if topic:
                    self.publish_json_rpc_message(topic, message = msg)
        # cleanup
        if service_id in self._read_stream_writers:
            logger.debug(f"Removing session for service_id: {service_id}")
            stream = self._read_stream_writers.pop(service_id)
            await stream.aclose()

        logger.debug(f"Session stream closed for service_id: {service_id}")

    def pick_service_id(self, service_name: str) -> ServiceId:
        return random.choice(list(self.service_list[service_name].keys()))

def validate_service_name(name: str):
    if "/" not in name:
        raise ValueError(f"Invalid service name: {name}, must contain a '/'")
    elif ("+" in name) or ("#" in name):
        raise ValueError(f"Invalid service name: {name}, must not contain '+' or '#'")
    elif name[0] == "/":
        raise ValueError(f"Invalid service name: {name}, must not start with '/'")
