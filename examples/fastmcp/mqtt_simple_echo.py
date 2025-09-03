"""
FastMCP Echo Server
"""

from mcp.server.fastmcp import FastMCP
from mcp.shared.mqtt import MqttOptions

# Create server
mcp = FastMCP(
    "demo_server/echo",
    log_level="DEBUG",
)

mcp.settings.mqtt_options = MqttOptions(
    host="broker.emqx.io",
    verify_connack_properties=True,  # Change to False if broker is Mosquitto
)


@mcp.tool()
def echo(text: str) -> str:
    """Echo the input text"""
    return text


if __name__ == "__main__":
    mcp.run(transport="mqtt")
