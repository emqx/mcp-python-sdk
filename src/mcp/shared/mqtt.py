"""
MQTT Transport Base Module

"""
from types import TracebackType
import paho.mqtt.client as mqtt
import logging
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import anyio
import anyio.from_thread as anyio_from_thread
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic import BaseModel, SecretStr
from typing import Literal, Optional, Any, TypeAlias, Callable, Awaitable
import mcp.types as types
from typing_extensions import Self
from abc import ABC, abstractmethod

DEFAULT_LOG_FORMAT = "%(asctime)s - %(message)s"
QOS = 1
MCP_SERVER_NAME = "MCP-SERVER-NAME"
MCP_AUTH_ROLE = "MCP-AUTH-ROLE"
PROPERTY_K_MCP_COMPONENT = "MCP-COMPONENT-TYPE"
PROPERTY_K_MQTT_CLIENT_ID = "MQTT-CLIENT-ID"
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
    password: Optional[SecretStr] = None
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

class MqttTransportBase(ABC):
    _read_stream_writers: dict[
        str, SndStreamEX
    ]

    def __init__(self, 
                 mcp_component_type: Literal["mcp-client", "mcp-server"],
                 mqtt_clientid: str | None = None,
                 mqtt_options: MqttOptions = MqttOptions(),
                 disconnected_msg: types.JSONRPCMessage | None = None,
                 disconnected_msg_retain: bool = True):
        self._read_stream_writers = {}
        self.mqtt_clientid = mqtt_clientid
        self.mcp_component_type = mcp_component_type
        self.mqtt_options = mqtt_options
        self.disconnected_msg = disconnected_msg
        self.disconnected_msg_retain = disconnected_msg_retain
        client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=mqtt_clientid, protocol=mqtt.MQTTv5,
            userdata={},
            transport=mqtt_options.transport, reconnect_on_failure=True
        )
        client.username_pw_set(mqtt_options.username, mqtt_options.password.get_secret_value() if mqtt_options.password else None)
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
        ## We need to set an empty will message to clean the retained presence
        ## message when the MCP server goes offline.
        ## Note that if the broker suggested a new server name, it's the broker's
        ## responsibility to clean the retained presence message and send the
        ## last will message on the changed presence topic.
        client.will_set(
            topic = self.get_presence_topic(),
            payload = disconnected_msg.model_dump_json() if disconnected_msg else None,
            qos = QOS,
            retain = disconnected_msg_retain,
            properties = self.get_publish_properties(),
        )
        logger.info(f"MCP component type: {mcp_component_type}, MQTT clientid: {mqtt_clientid}, MQTT settings: {mqtt_options}")
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
        await self.stop_mqtt()
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

    def publish_json_rpc_message(self, topic: str, message: types.JSONRPCMessage | None,
                                 retain: bool = False):
        props = self.get_publish_properties()
        payload = message.model_dump_json(by_alias=True, exclude_none=True) if message else None
        self.client.publish(topic=topic, payload=payload, qos=QOS, retain=retain, properties=props)

    def get_publish_properties(self):
        props = Properties(PacketTypes.PUBLISH)
        props.UserProperty = [
            (PROPERTY_K_MCP_COMPONENT, self.mcp_component_type),
            (PROPERTY_K_MQTT_CLIENT_ID, self.mqtt_clientid)
        ]
        return props

    def connect(self):
        logger.debug("Setting up MQTT connection")
        props = Properties(PacketTypes.CONNECT)
        props.UserProperty = [
            (PROPERTY_K_MCP_COMPONENT, self.mcp_component_type)
        ]
        self.client.connect(
            host = self.mqtt_options.host,
            port = self.mqtt_options.port,
            keepalive = self.mqtt_options.keepalive,
            bind_address = self.mqtt_options.bind_address,
            bind_port = self.mqtt_options.bind_port,
            clean_start=True,
            properties=props,
        )

    def assert_property(self, properties: Properties | None, property_name: str, expected_value: Any):
        if get_property(properties, property_name) == expected_value:
            pass
        else:
            anyio_from_thread.run(self.stop_mqtt)
            raise ValueError(f"{property_name} not available")

    @abstractmethod
    def get_presence_topic(self) -> str:
        pass

    async def stop_mqtt(self):
        self.publish_json_rpc_message(
            self.get_presence_topic(),
            message = self.disconnected_msg,
            retain = self.disconnected_msg_retain
        )
        self.client.disconnect()
        self.client.loop_stop()
        for stream in self._read_stream_writers.values():
            await stream.aclose()
        self._read_stream_writers = {}
        logger.debug("Disconnected from MQTT broker_host")

def get_property(properties: Properties | None, property_name: str):
    if properties and hasattr(properties, property_name):
        return getattr(properties, property_name)
    else:
        return False

def configure_logging(
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO",
    format: str = DEFAULT_LOG_FORMAT,
) -> None:
    handlers: list[logging.Handler] = []
    try:
        from rich.console import Console
        from rich.logging import RichHandler

        handlers.append(RichHandler(console=Console(stderr=True), rich_tracebacks=True))
    except ImportError:
        pass

    if not handlers:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=level,
        format=format,
        handlers=handlers,
    )
