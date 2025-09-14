"""
This module implements the MQTT transport for the MCP server.
"""

import asyncio
import json
import logging
import traceback
from collections.abc import Awaitable, Callable
from typing import Any, TypeAlias
from uuid import uuid4

import anyio
import anyio.from_thread as anyio_from_thread
import anyio.to_thread as anyio_to_thread
import paho.mqtt.client as mqtt
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.subscribeoptions import SubscribeOptions

import mcp.shared.mqtt_topic as mqtt_topic
import mcp.types as types
from mcp.shared.message import SessionMessage
from mcp.shared.mqtt import MCP_SERVER_NAME, PROPERTY_K_MQTT_CLIENT_ID, QOS, MqttOptions, MqttTransportBase

# Raw MQTT streams (JSONRPCMessage)
RcvStream: TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage]
SndStream: TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage]
RcvStreamEx: TypeAlias = MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
SndStreamEX: TypeAlias = MemoryObjectSendStream[types.JSONRPCMessage | Exception]

# Session streams (SessionMessage)
SessionRcvStream: TypeAlias = MemoryObjectReceiveStream[SessionMessage]
SessionSndStream: TypeAlias = MemoryObjectSendStream[SessionMessage]
SessionRcvStreamEx: TypeAlias = MemoryObjectReceiveStream[SessionMessage | Exception]
SessionSndStreamEx: TypeAlias = MemoryObjectSendStream[SessionMessage | Exception]

ServerSessionRun: TypeAlias = Callable[[SessionRcvStreamEx, SessionSndStream], Awaitable[Any]]

logger = logging.getLogger(__name__)


class MqttTransportServer(MqttTransportBase):
    def __init__(
        self,
        server_session_run: ServerSessionRun,
        server_name: str,
        server_description: str,
        server_meta: dict[str, Any],
        client_id: str | None = None,
        mqtt_options: MqttOptions = MqttOptions(),
    ):
        uuid = uuid4().hex
        mqtt_clientid = client_id if client_id else uuid
        self.server_id = mqtt_clientid
        self.server_name = server_name
        self.server_description = server_description
        self.server_meta = server_meta
        self.server_session_run = server_session_run
        super().__init__(
            "mcp-server",
            mqtt_clientid=mqtt_clientid,
            mqtt_options=mqtt_options,
            disconnected_msg=None,
            disconnected_msg_retain=True,
        )

    def get_presence_topic(self) -> str:
        return mqtt_topic.get_server_presence_topic(self.server_id, self.server_name)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        connect_flags: mqtt.ConnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ):
        super()._on_connect(client, userdata, connect_flags, reason_code, properties)
        if reason_code == 0:
            if properties and hasattr(properties, "UserProperty"):
                user_properties: dict[str, Any] = dict(properties.UserProperty)  # type: ignore
                if MCP_SERVER_NAME in user_properties:
                    broker_suggested_server_name = user_properties[MCP_SERVER_NAME]
                    self.server_name = broker_suggested_server_name
                    logger.debug(f"Used broker suggested server name: {broker_suggested_server_name}")
                else:
                    logger.error(f"No {PROPERTY_K_MQTT_CLIENT_ID} in UserProperties")
            self.server_control_topic = mqtt_topic.get_server_control_topic(self.server_id, self.server_name)
            ## Subscribe to the server control topic
            client.subscribe(self.server_control_topic, QOS)
            ## Reister the server on the presence topic
            online_msg = types.JSONRPCMessage(
                types.JSONRPCNotification(
                    jsonrpc="2.0",
                    method="notifications/server/online",
                    params={"description": self.server_description, "meta": self.server_meta},
                )
            )
            self.publish_json_rpc_message(self.get_presence_topic(), message=online_msg, retain=True)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        logger.debug(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        match msg.topic:
            case str() as t if t == self.server_control_topic:
                self.handle_server_contorl_message(msg)
            case str() as t if t.startswith(mqtt_topic.CLIENT_CAPABILITY_CHANGE_BASE):
                self.handle_client_capability_change_message(msg)
            case str() as t if t.startswith(mqtt_topic.RPC_BASE):
                self.handle_rpc_message(msg)
            case str() as t if t.startswith(mqtt_topic.CLIENT_PRESENCE_BASE):
                self.handle_client_presence_message(msg)
            case _:
                logger.error(f"Received message on unexpected topic: {msg.topic}")

    def _on_subscribe(
        self,
        client: mqtt.Client,
        userdata: Any,
        mid: int,
        reason_code_list: list[ReasonCode],
        properties: Properties | None,
    ):
        if mid in userdata.get("pending_subs", {}):
            mcp_client_id, msg, rpc_msg_id = userdata["pending_subs"].pop(mid)
            ## only create session if all topic subscribed successfully
            if all(rc.value == QOS for rc in reason_code_list):
                logger.debug(f"Subscribed to topics for mcp_client_id: {mcp_client_id}")
                anyio_from_thread.run(self.create_session, mcp_client_id, msg)
            else:
                logger.error(
                    f"Failed to subscribe to topics for mcp_client_id: {mcp_client_id}, "
                    f"reason_codes: {reason_code_list}"
                )
                err = types.JSONRPCError(
                    jsonrpc="2.0",
                    id=rpc_msg_id,
                    error=types.ErrorData(code=types.INTERNAL_ERROR, message="Failed to subscribe to client topics"),
                )
                self.publish_json_rpc_message(
                    mqtt_topic.get_rpc_topic(mcp_client_id, self.server_id, self.server_name),
                    message=types.JSONRPCMessage(err),
                )

    def handle_server_contorl_message(self, msg: mqtt.MQTTMessage):
        if msg.properties and hasattr(msg.properties, "UserProperty"):
            user_properties: dict[str, Any] = dict(msg.properties.UserProperty)  # type: ignore
            if PROPERTY_K_MQTT_CLIENT_ID in user_properties:
                mcp_client_id = user_properties[PROPERTY_K_MQTT_CLIENT_ID]
                if mcp_client_id in self._read_stream_writers:
                    anyio_from_thread.run(self._send_message_to_session, mcp_client_id, msg)
                else:
                    self.maybe_subscribe_to_client(mcp_client_id, msg)
            else:
                logger.error(f"No {PROPERTY_K_MQTT_CLIENT_ID} in UserProperties")
        else:
            logger.error("No UserProperties in control message")

    def handle_client_capability_change_message(self, msg: mqtt.MQTTMessage) -> None:
        mcp_client_id = msg.topic.split("/")[-1]
        anyio_from_thread.run(self._send_message_to_session, mcp_client_id, msg)

    def handle_rpc_message(self, msg: mqtt.MQTTMessage) -> None:
        mcp_client_id = msg.topic.split("/")[1]
        try:
            json_msg = json.loads(msg.payload.decode())
            if "method" in json_msg:
                if json_msg["method"] == "notifications/disconnected":
                    stream = self._read_stream_writers[mcp_client_id]
                    anyio_from_thread.run(stream.aclose)
                    logger.debug(f"Closed read_stream for mcp_client_id: {mcp_client_id}")
                    return
                else:
                    anyio_from_thread.run(self._send_message_to_session, mcp_client_id, msg)
            else:
                anyio_from_thread.run(self._send_message_to_session, mcp_client_id, msg)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in RPC message for mcp_client_id: {mcp_client_id}")

    def handle_client_presence_message(self, msg: mqtt.MQTTMessage) -> None:
        mcp_client_id = msg.topic.split("/")[-1]
        if mcp_client_id not in self._read_stream_writers:
            logger.error(f"No session for mcp_client_id: {mcp_client_id}")
            return
        try:
            json_msg = json.loads(msg.payload.decode())
            if "method" in json_msg:
                if json_msg["method"] == "notifications/disconnected":
                    stream = self._read_stream_writers[mcp_client_id]
                    anyio_from_thread.run(stream.aclose)
                    logger.debug(f"Closed read_stream for mcp_client_id: {mcp_client_id}")
                else:
                    logger.error(f"Unknown method in presence message for mcp_client_id: {mcp_client_id}")
            else:
                logger.error(f"No method in presence message for mcp_client_id: {mcp_client_id}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in presence message for mcp_client_id: {mcp_client_id}")

    async def create_session(self, mcp_client_id: str, msg: mqtt.MQTTMessage):
        ## Streams are used to communicate between the MqttTransportServer and the MCPSession:
        ## 1. MQTT --> Server -->[raw_read]-- conversion -->[session_read]--> MCPSession
        ## 2. MQTT <-- Server <--[raw_write]<-- conversion <--[session_write]-- MCPSession

        # Create raw MQTT streams (JSONRPCMessage)
        raw_read_stream_writer: SndStreamEX
        raw_read_stream: RcvStreamEx
        raw_write_stream: SndStream
        raw_write_stream_reader: RcvStream
        raw_read_stream_writer, raw_read_stream = anyio.create_memory_object_stream(0)  # type: ignore
        raw_write_stream, raw_write_stream_reader = anyio.create_memory_object_stream(0)  # type: ignore

        # Create session streams (SessionMessage)
        session_read_stream_writer: SessionSndStreamEx
        session_read_stream: SessionRcvStreamEx
        session_write_stream: SessionSndStream
        session_write_stream_reader: SessionRcvStream
        session_read_stream_writer, session_read_stream = anyio.create_memory_object_stream(0)  # type: ignore
        session_write_stream, session_write_stream_reader = anyio.create_memory_object_stream(0)  # type: ignore

        # Start conversion tasks
        self._task_group.start_soon(self._convert_jsonrpc_to_session, raw_read_stream, session_read_stream_writer)
        self._task_group.start_soon(self._convert_session_to_jsonrpc, session_write_stream_reader, raw_write_stream)

        self._read_stream_writers[mcp_client_id] = raw_read_stream_writer
        self._task_group.start_soon(self.server_session_run, session_read_stream, session_write_stream)
        self._task_group.start_soon(self._receieved_from_session, mcp_client_id, raw_write_stream_reader)
        logger.debug(f"Created new session for mcp_client_id: {mcp_client_id}")
        await self._send_message_to_session(mcp_client_id, msg)

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
            (
                mqtt_topic.get_rpc_topic(mcp_client_id, self.server_id, self.server_name),
                SubscribeOptions(qos=QOS, noLocal=True),
            ),
        ]
        ret, mid = self.client.subscribe(topic=topic_filters)
        if ret != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to subscribe to topics for mcp_client_id: {mcp_client_id}")
            return
        userdata = self.client.user_data_get()
        pending_subs = userdata.get("pending_subs", {})
        pending_subs[mid] = (mcp_client_id, msg, rcp_msg_id)
        userdata["pending_subs"] = pending_subs

    async def _send_message_to_session(self, mcp_client_id: str, msg: mqtt.MQTTMessage):
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
            # await read_stream_writer.send(exc)

    async def _receieved_from_session(self, mcp_client_id: str, write_stream_reader: RcvStream):
        async with write_stream_reader:
            async for msg in write_stream_reader:
                logger.debug(f"Got msg from session for mcp_client_id: {mcp_client_id}, msg: {msg}")
                match msg.model_dump():
                    case {"method": "notifications/resources/updated"}:
                        logger.warning("Resource updates should not be sent from the session. Ignoring.")
                    case {"method": method} if method.endswith("/list_changed"):
                        logger.warning("Resource updates should not be sent from the session. Ignoring.")
                    case _:
                        topic = mqtt_topic.get_rpc_topic(mcp_client_id, self.server_id, self.server_name)
                        self.publish_json_rpc_message(topic, message=msg)
        # cleanup
        if mcp_client_id in self._read_stream_writers:
            logger.debug(f"Removing session for mcp_client_id: {mcp_client_id}")
            stream = self._read_stream_writers.pop(mcp_client_id)
            await stream.aclose()

        # unsubscribe from the client topics
        logger.debug(f"Unsubscribing from topics for mcp_client_id: {mcp_client_id}")
        topic_filters = [
            mqtt_topic.get_client_presence_topic(mcp_client_id),
            mqtt_topic.get_client_capability_change_topic(mcp_client_id),
            mqtt_topic.get_rpc_topic(mcp_client_id, self.server_id, self.server_name),
        ]
        self.client.unsubscribe(topic=topic_filters)

        logger.debug(f"Session stream closed for mcp_client_id: {mcp_client_id}")

    async def _convert_jsonrpc_to_session(
        self,
        jsonrpc_stream: RcvStreamEx,
        session_writer: SessionSndStreamEx,
    ) -> None:
        """Convert JSONRPCMessage stream to SessionMessage stream."""
        async with jsonrpc_stream, session_writer:
            async for message in jsonrpc_stream:
                if isinstance(message, Exception):
                    await session_writer.send(message)
                else:
                    session_message = SessionMessage(message=message)
                    await session_writer.send(session_message)

    async def _convert_session_to_jsonrpc(
        self,
        session_stream: SessionRcvStream,
        jsonrpc_writer: SndStream,
    ) -> None:
        """Convert SessionMessage stream to JSONRPCMessage stream."""
        async with session_stream, jsonrpc_writer:
            async for session_message in session_stream:
                await jsonrpc_writer.send(session_message.message)


async def start_mqtt(
    server_session_run: ServerSessionRun,
    server_name: str,
    server_description: str,
    server_meta: dict[str, Any],
    client_id: str | None = None,
    mqtt_options: MqttOptions = MqttOptions(),
):
    async with MqttTransportServer(
        server_session_run,
        server_name=server_name,
        server_description=server_description,
        server_meta=server_meta,
        client_id=client_id,
        mqtt_options=mqtt_options,
    ) as mqtt_trans:

        def start():
            mqtt_trans.connect()
            mqtt_trans.client.loop_forever()

        try:
            await anyio_to_thread.run_sync(start)
        except asyncio.CancelledError:
            logger.debug("MQTT transport (MCP server) got cancelled")
        except Exception as exc:
            logger.error(f"MQTT transport (MCP server) failed with exception: {exc}")


def validate_server_name(name: str):
    if "/" not in name:
        raise ValueError(f"Invalid server name: {name}, must contain a '/'")
    elif ("+" in name) or ("#" in name):
        raise ValueError(f"Invalid server name: {name}, must not contain '+' or '#'")
    elif name[0] == "/":
        raise ValueError(f"Invalid server name: {name}, must not start with '/'")
