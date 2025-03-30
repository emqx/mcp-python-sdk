"""
SSE Server Transport Module

This module implements a Server-Sent Events (SSE) transport layer for MCP servers."
"""

import asyncio
import json
import traceback
from types import TracebackType
import mcp.shared.mqtt_channel as mqtt_channel
import paho.mqtt.client as mqtt
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions
from uuid import uuid4
import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic import BaseModel
from typing import Literal, Optional, Any, TypeAlias, Callable, Awaitable
import mcp.types as types
from typing_extensions import Self

QOS = 1
logger = logging.getLogger(__name__)

RcvStream : TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage]
SndStream : TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage]
RcvStreamEx : TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
SndStreamEX : TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage | Exception]
ServerRun : TypeAlias = Callable[[RcvStreamEx, SndStream], Awaitable[Any]]

class MqttOptions(BaseModel):
    host: str = "localhost"
    port: int = 1883
    transport: Literal['tcp', 'websockets', 'unix'] = 'tcp'
    keepalive: int = 60
    bind_address: str = ''
    bind_port: int = 0
    username: Optional[str] = None
    password: Optional[str] = None
    tls_enabled: bool = False
    tls_version: Optional[int] = None
    tls_insecure: bool = False
    ca_certs: Optional[str] = None
    certfile: Optional[str] = None
    keyfile: Optional[str] = None
    ciphers: Optional[str] = None
    keyfile_password: Optional[str] = None
    alpn_protocols: Optional[list[str]] = None
    websocket_path: str = '/mqtt'
    websocket_headers: Optional[dict[str, str]] = None

class MqttTransport:
    _read_stream_writers: dict[
        str, SndStreamEX
    ]

    def __init__(self, server_run: ServerRun, service_name: str,
                 service_description: str,
                 service_meta: dict[str, Any],
                 client_id_prefix: str | None = None,
                 mqtt_options: MqttOptions = MqttOptions()):
        self._read_stream_writers = {}
        self.resource_ids: dict[str, str] = {}
        uuid = uuid4().hex
        service_id = f"{client_id_prefix}-{uuid}" if client_id_prefix else uuid
        self.mqtt_options = mqtt_options
        self.service_name = service_name
        self.service_description = service_description
        self.service_meta = service_meta
        self.service_id = service_id
        self.service_control_channel = mqtt_channel.get_service_control_channel(service_name)
        self.service_presence_channel = mqtt_channel.get_service_presence_channel(service_id, service_name)
        self.service_capability_change_channel = mqtt_channel.get_service_capability_change_channel(service_id, service_name)
        self.server_run = server_run
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=service_id, protocol=mqtt.MQTTv5,
            userdata={},
            transport=mqtt_options.transport, reconnect_on_failure=True
        )
        client.username_pw_set(mqtt_options.username, mqtt_options.password)
        if mqtt_options.tls_enabled:
            client.tls_set( # type: ignore
                ca_certs=mqtt_options.ca_certs,
                certfile=mqtt_options.certfile,
                keyfile=mqtt_options.keyfile,
                tls_version=mqtt_options.tls_version,
                ciphers=mqtt_options.ciphers,
                keyfile_password=mqtt_options.keyfile_password,
                alpn_protocols=mqtt_options.alpn_protocols
            )
            client.tls_insecure_set(mqtt_options.tls_insecure)
        if mqtt_options.transport == 'websockets':
            client.ws_set_options(path=mqtt_options.websocket_path, headers=mqtt_options.websocket_headers)
        client.will_set(topic=self.service_presence_channel, payload=None, qos=QOS, retain=True)
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.on_subscribe = self._on_subscribe
        self.client = client

    async def __aenter__(self) -> Self:
        self._task_group = anyio.create_task_group()
        await self._task_group.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        self._task_group.cancel_scope.cancel()
        return await self._task_group.__aexit__(exc_type, exc_val, exc_tb)

    def _on_connect(self, client: mqtt.Client, userdata: Any, connect_flags: mqtt.ConnectFlags, reason_code : ReasonCode, properties: Properties | None):
        if reason_code == 0:
            logger.debug(f"Connected to MQTT broker_host at {self.mqtt_options.host}:{self.mqtt_options.port}")
            self.assert_property(properties, "RetainAvailable", 1)
            self.assert_property(properties, "WildcardSubscriptionAvailable", 1)
            ## Subscribe to the service control channel
            client.subscribe(self.service_control_channel, QOS)
            ## Reister the service on the presence channel
            online_msg = types.JSONRPCNotification(
                jsonrpc="2.0",
                method = "notifications/service/online",
                params = {
                    "description": self.service_description,
                    "meta": self.service_meta
                }
            )
            client.publish(self.service_presence_channel,
                           payload=online_msg.model_dump_json(), qos=QOS, retain=True)
        else:
            logger.error(f"Failed to connect, return code {reason_code}")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        logger.debug(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        match msg.topic:
            case str() as t if t == self.service_control_channel:
                self.handle_service_contorl_message(msg)
            case str() as t if t.startswith(mqtt_channel.CLIENT_CAPABILITY_CHANGE_BASE):
                self.handle_client_capability_change_message(msg)
            case str() as t if t.startswith(mqtt_channel.RPC_BASE):
                self.handle_rpc_message(msg)
            case str() as t if t.startswith(mqtt_channel.CLIENT_PRESENCE_BASE):
                self.handle_client_presence_message(msg)
            case _:
                logger.error(f"Received message on unexpected topic: {msg.topic}")

    def _on_subscribe(self, client: mqtt.Client, userdata: Any, mid: int,
                      reason_code_list: list[ReasonCode], properties: Properties | None):
        if mid in userdata.get("pending_subs", {}):
            mcp_client_id, msg, rpc_msg_id = userdata["pending_subs"].pop(mid)
            ## only create session if all topic subscribed successfully
            if all([rc.value == QOS for rc in reason_code_list]):
                logger.debug(f"Subscribed to topics for mcp_client_id: {mcp_client_id}")
                anyio.from_thread.run(self.create_session, mcp_client_id, msg)
            else:
                logger.error(f"Failed to subscribe to topics for mcp_client_id: {mcp_client_id}, reason_codes: {reason_code_list}")
                err = types.JSONRPCError(
                    jsonrpc="2.0",
                    id=rpc_msg_id,
                    error=types.ErrorData(
                        code=types.INTERNAL_ERROR,
                        message="Failed to subscribe to client topics"
                    )
                )
                self.publish_json_rpc_message(
                    mqtt_channel.get_rpc_channel(mcp_client_id, self.service_name),
                    types.JSONRPCMessage(err)
                )

    def handle_service_contorl_message(self, msg: mqtt.MQTTMessage):
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            user_properties: dict[str, Any] = dict(msg.properties.UserProperty) # type: ignore
            if "mcp_client_id" in user_properties:
                mcp_client_id = user_properties["mcp_client_id"]
                if mcp_client_id in self._read_stream_writers:
                    anyio.from_thread.run(self.send_message_to_session, mcp_client_id, msg)
                else:
                    self.maybe_subscribe_to_client(mcp_client_id, msg)
            else:
                logger.error("No mcp_client_id in UserProperties")
        else:
            logger.error("No UserProperties in control message")

    def handle_client_capability_change_message(self, msg: mqtt.MQTTMessage) -> None:
        mcp_client_id = msg.topic.split("/")[-1]
        anyio.from_thread.run(self.send_message_to_session, mcp_client_id, msg)

    def handle_rpc_message(self, msg: mqtt.MQTTMessage) -> None:
        mcp_client_id = msg.topic.split("/")[1]
        anyio.from_thread.run(self.send_message_to_session, mcp_client_id, msg)

    def handle_client_presence_message(self, msg: mqtt.MQTTMessage) -> None:
        mcp_client_id = msg.topic.split("/")[-1]
        if mcp_client_id not in self._read_stream_writers:
            logger.error(f"No session for mcp_client_id: {mcp_client_id}")
            return
        try:
            json_msg = json.loads(msg.payload.decode())
            if "method" in json_msg:
                if json_msg["method"] == "notifications/disconnected":
                    stream = self._read_stream_writers.pop(mcp_client_id)
                    anyio.from_thread.run(stream.aclose)
                    logger.debug(f"Removed session for mcp_client_id: {mcp_client_id}")
                else:
                    logger.error(f"Unknown method in control message for mcp_client_id: {mcp_client_id}")
            else:
                logger.error(f"No method in control message for mcp_client_id: {mcp_client_id}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in control message for mcp_client_id: {mcp_client_id}")

    async def create_session(self, mcp_client_id: str, msg: mqtt.MQTTMessage):
        ## Streams are used to communicate between the MqttTransport and the MCPSession:
        ## 1. (msg) --> MqttBroker --> MqttTransport -->[read_stream_writer]-->[read_stream]--> MCPSession
        ## 2. MqttBroker <-- MqttTransport <--[write_stream_reader]--[write_stream]-- MCPSession <-- (msg)
        read_stream: RcvStreamEx
        read_stream_writer: SndStreamEX
        write_stream: SndStream
        write_stream_reader: RcvStream
        read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
        write_stream, write_stream_reader = anyio.create_memory_object_stream(0)
        self._read_stream_writers[mcp_client_id] = read_stream_writer
        self._task_group.start_soon(self.server_run, read_stream, write_stream)
        self._task_group.start_soon(self.receieved_from_session, mcp_client_id, write_stream_reader)
        logger.debug(f"Created new session for mcp_client_id: {mcp_client_id}")
        await self.send_message_to_session(mcp_client_id, msg)

    def maybe_subscribe_to_client(self, mcp_client_id: str, msg: mqtt.MQTTMessage):
        try:
            json_msg = json.loads(msg.payload.decode())
            if "id" in json_msg:
                rpc_msg_id = json_msg["id"]
                self.subscribe_to_client(mcp_client_id, msg, rpc_msg_id)
            else:
                logger.error(f"No id in control message for mcp_client_id: {mcp_client_id}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in control message for mcp_client_id: {mcp_client_id}")
            return

    def subscribe_to_client(self, mcp_client_id: str, msg: mqtt.MQTTMessage, rcp_msg_id: Any):
        topic_filters = [
            (mqtt_channel.get_client_presence_channel(mcp_client_id), SubscribeOptions(qos=QOS)),
            (mqtt_channel.get_client_capability_change_channel(mcp_client_id), SubscribeOptions(qos=QOS)),
            (mqtt_channel.get_rpc_channel(mcp_client_id, self.service_name), SubscribeOptions(qos=QOS, noLocal=True))
        ]
        ret, mid = self.client.subscribe(topic=topic_filters)
        if ret != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to subscribe to topics for mcp_client_id: {mcp_client_id}")
            return
        userdata = self.client.user_data_get()
        pending_subs = userdata.get("pending_subs", {})
        pending_subs[mid] = (mcp_client_id, msg, rcp_msg_id)
        userdata["pending_subs"] = pending_subs

    async def send_message_to_session(self, mcp_client_id: str, msg: mqtt.MQTTMessage):
        payload = msg.payload.decode()
        if mcp_client_id not in self._read_stream_writers:
            logger.error(f"No session for mcp_client_id: {mcp_client_id}")
            return
        read_stream_writer = self._read_stream_writers[mcp_client_id]
        try:
            message = types.JSONRPCMessage.model_validate_json(payload)
            logger.debug(f"Sending msg to session for mcp_client_id: {mcp_client_id}, msg: {message}")
            with anyio.fail_after(3):
                await read_stream_writer.send(message)
        except Exception as exc:
            logger.error(f"Failed to send msg to session for mcp_client_id: {mcp_client_id}, exception: {exc}")
            traceback.print_exc()
            ## TODO: the session does not handle exceptions for now
            #await read_stream_writer.send(exc)

    async def receieved_from_session(self, mcp_client_id: str, write_stream_reader: RcvStream):
        async with write_stream_reader:
            async for msg in write_stream_reader:
                logger.debug(f"Got msg from session for mcp_client_id: {mcp_client_id}, msg: {msg}")
                match msg.model_dump():
                    case {"method": "notifications/resources/updated", "params": {"uri": uri}}:
                        ## Mantain a mapping of resource_id to uri
                        resource_id = uuid4().hex
                        self.resource_ids[resource_id] = uri
                        topic = mqtt_channel.get_service_resource_update_channel(self.service_id, resource_id)
                    case {"method": method} if method.endswith("/list_changed"):
                        topic = mqtt_channel.get_service_capability_change_channel(self.service_id, self.service_name)
                    case _:
                        topic = mqtt_channel.get_rpc_channel(mcp_client_id, self.service_name)
                self.publish_json_rpc_message(topic, msg)
        # cleanup
        if mcp_client_id in self._read_stream_writers:
            logger.debug(f"Removing session for mcp_client_id: {mcp_client_id}")
            stream = self._read_stream_writers.pop(mcp_client_id)
            await stream.aclose()

        logger.debug(f"Session stream closed for mcp_client_id: {mcp_client_id}")

    def publish_json_rpc_message(self, topic: str, message: types.JSONRPCMessage):
        json = message.model_dump_json(by_alias=True, exclude_none=True)
        self.client.publish(topic, json, qos=QOS)

    def connect(self):
        logger.debug("Setting up MQTT connection")
        self.client.connect(
            host = self.mqtt_options.host,
            port = self.mqtt_options.port,
            keepalive = self.mqtt_options.keepalive,
            bind_address = self.mqtt_options.bind_address,
            bind_port = self.mqtt_options.bind_port,
            clean_start=True
        )

    def assert_property(self, properties: Properties | None, property_name: str, expected_value: Any):
        if get_property(properties, property_name) == expected_value:
            pass
        else:
            self.stop_mqtt()
            raise ValueError(f"{property_name} not available")

    def stop_mqtt(self):
        self.client.publish(self.service_presence_channel, payload=None, qos=QOS, retain=True)
        self.client.disconnect()
        self.client.loop_stop()
        for stream in self._read_stream_writers.values():
            anyio.from_thread.run(stream.aclose)
        self._read_stream_writers = {}
        logger.debug("Disconnected from MQTT broker_host")

def get_property(properties: Properties | None, property_name: str):
    if properties and hasattr(properties, property_name):
        return getattr(properties, property_name)
    else:
        return False

async def start_mqtt(
        server_run: ServerRun, service_name: str,
        service_description: str,
        service_meta: dict[str, Any],
        client_id_prefix: str | None = None,
        mqtt_options: MqttOptions = MqttOptions()):
    async with MqttTransport(
        server_run,
        service_name = service_name,
        service_description=service_description,
        service_meta = service_meta,
        client_id_prefix = client_id_prefix,
        mqtt_options = mqtt_options
    ) as mqtt_trans:
        def start():
            mqtt_trans.connect()
            mqtt_trans.client.loop_forever()
        try:
            await anyio.to_thread.run_sync(start)
        except asyncio.CancelledError:
            logger.debug("MQTT transport got cancelled")

def validate_service_name(name: str):
    if "/" not in name:
        raise ValueError(f"Invalid service name: {name}, must contain a '/'")
    elif ("+" in name) or ("#" in name):
        raise ValueError(f"Invalid service name: {name}, must not contain '+' or '#'")
    elif name[0] == "/":
        raise ValueError(f"Invalid service name: {name}, must not start with '/'")
