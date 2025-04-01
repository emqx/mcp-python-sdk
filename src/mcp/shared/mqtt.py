"""
MQTT Transport Base Module

"""
from types import TracebackType
import paho.mqtt.client as mqtt
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
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

class MqttTransportBase:
    _read_stream_writers: dict[
        str, SndStreamEX
    ]

    def __init__(self, mqtt_clientid: str | None = None,
                 mqtt_options: MqttOptions = MqttOptions()):
        self._read_stream_writers = {}
        self.mqtt_clientid = mqtt_clientid
        self.mqtt_options = mqtt_options
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=mqtt_clientid, protocol=mqtt.MQTTv5,
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
        else:
            logger.error(f"Failed to connect, return code {reason_code}")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        pass

    def _on_subscribe(self, client: mqtt.Client, userdata: Any, mid: int,
                      reason_code_list: list[ReasonCode], properties: Properties | None):
        pass

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
        self.client.publish(self.service_presence_topic, payload=None, qos=QOS, retain=True)
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

