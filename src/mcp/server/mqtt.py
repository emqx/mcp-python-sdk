"""
This module implements the MQTT transport for the MCP server.
"""
from uuid import uuid4
from mcp.shared.mqtt import MqttTransportBase, MqttOptions, QOS
import asyncio
import json
import traceback
import mcp.shared.mqtt_topic as mqtt_topic
import paho.mqtt.client as mqtt
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions
import anyio
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from typing import Any, TypeAlias, Callable, Awaitable
import mcp.types as types

RcvStream : TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage]
SndStream : TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage]
RcvStreamEx : TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
SndStreamEX : TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage | Exception]
ServerRun : TypeAlias = Callable[[RcvStreamEx, SndStream], Awaitable[Any]]

PROPERTY_K_MCP_CLIENT_ID = "mcp-client-id"
logger = logging.getLogger(__name__)

class MqttTransport(MqttTransportBase):

    def __init__(self, server_run: ServerRun, service_name: str,
                 service_description: str,
                 service_meta: dict[str, Any],
                 client_id_prefix: str | None = None,
                 mqtt_options: MqttOptions = MqttOptions()):
        uuid = uuid4().hex
        mqtt_clientid = f"{client_id_prefix}-{uuid}" if client_id_prefix else uuid
        self.service_id = mqtt_clientid
        self.service_name = service_name
        self.service_description = service_description
        self.service_meta = service_meta
        self.service_control_topic = mqtt_topic.get_service_control_topic(service_name)
        self.service_presence_topic = mqtt_topic.get_service_presence_topic(self.service_id, service_name)
        self.service_capability_change_topic = mqtt_topic.get_service_capability_change_topic(self.service_id, service_name)
        self.server_run = server_run
        super().__init__(mqtt_clientid, mqtt_options)
        self.presence_topic = mqtt_topic.get_service_presence_topic(self.service_id, service_name)
        self.client.will_set(topic=self.presence_topic, payload=None, qos=QOS, retain=True)

    def _on_connect(self, client: mqtt.Client, userdata: Any, connect_flags: mqtt.ConnectFlags, reason_code : ReasonCode, properties: Properties | None):
        if reason_code == 0:
            super()._on_connect(client, userdata, connect_flags, reason_code, properties)
            ## Subscribe to the service control topic
            client.subscribe(self.service_control_topic, QOS)
            ## Reister the service on the presence topic
            online_msg = types.JSONRPCNotification(
                jsonrpc="2.0",
                method = "notifications/service/online",
                params = {
                    "description": self.service_description,
                    "meta": self.service_meta
                }
            )
            client.publish(self.presence_topic, payload=online_msg.model_dump_json(),
                        qos=QOS, retain=True)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        logger.debug(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        match msg.topic:
            case str() as t if t == self.service_control_topic:
                self.handle_service_contorl_message(msg)
            case str() as t if t.startswith(mqtt_topic.CLIENT_CAPABILITY_CHANGE_BASE):
                self.handle_client_capability_change_message(msg)
            case str() as t if t.startswith(mqtt_topic.RPC_BASE):
                self.handle_rpc_message(msg)
            case str() as t if t.startswith(mqtt_topic.CLIENT_PRESENCE_BASE):
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
                    mqtt_topic.get_rpc_topic(mcp_client_id, self.service_name),
                    types.JSONRPCMessage(err)
                )

    def handle_service_contorl_message(self, msg: mqtt.MQTTMessage):
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            user_properties: dict[str, Any] = dict(msg.properties.UserProperty) # type: ignore
            if PROPERTY_K_MCP_CLIENT_ID in user_properties:
                mcp_client_id = user_properties[PROPERTY_K_MCP_CLIENT_ID]
                if mcp_client_id in self._read_stream_writers:
                    anyio.from_thread.run(self.send_message_to_session, mcp_client_id, msg)
                else:
                    self.maybe_subscribe_to_client(mcp_client_id, msg)
            else:
                logger.error(f"No {PROPERTY_K_MCP_CLIENT_ID} in UserProperties")
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
            (mqtt_topic.get_client_presence_topic(mcp_client_id), SubscribeOptions(qos=QOS)),
            (mqtt_topic.get_client_capability_change_topic(mcp_client_id), SubscribeOptions(qos=QOS)),
            (mqtt_topic.get_rpc_topic(mcp_client_id, self.service_name), SubscribeOptions(qos=QOS, noLocal=True))
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
                    case {"method": "notifications/resources/updated"}:
                        logger.warning("Resource updates should not be sent from the session. Ignoring.")
                    case {"method": method} if method.endswith("/list_changed"):
                        logger.warning("Resource updates should not be sent from the session. Ignoring.")
                    case _:
                        topic = mqtt_topic.get_rpc_topic(mcp_client_id, self.service_name)
                        self.publish_json_rpc_message(topic, msg)
        # cleanup
        if mcp_client_id in self._read_stream_writers:
            logger.debug(f"Removing session for mcp_client_id: {mcp_client_id}")
            stream = self._read_stream_writers.pop(mcp_client_id)
            await stream.aclose()

        logger.debug(f"Session stream closed for mcp_client_id: {mcp_client_id}")

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
