"""
FastMCP Echo Server
"""

import os
from mcp.server.fastmcp import FastMCP
from mcp.server.mqtt import MqttOptions

status = "off"

# Create server
mcp = FastMCP(
    "devices/light",
    log_level="DEBUG",
    mqtt_server_description="A simple FastMCP server that controls a light device. You can turn the light on and off, and change its brightness.",
    mqtt_client_id = os.getenv("MQTT_CLIENT_ID"),
    mqtt_options= MqttOptions(
        username="aaa",
        host="localhost",
        port=1883,
    ),
)

@mcp.tool()
def change_brightness(level: int) -> str:
    """Change the brightness of the light, level should be between 0 and 100"""
    if 0 <= level <= 100:
        return f"Changed brightness to {level}"
    return "Invalid brightness level. Please provide a level between 0 and 100."

@mcp.tool()
def turn_on() -> str:
    """Turn the light on"""
    global status
    if status == "on":
        return "OK, but the light is already on"
    status = "on"
    return "Light turned on"

@mcp.tool()
def turn_off() -> str:
    """Turn the light off"""
    global status
    if status == "off":
        return "OK, but the light is already off"
    status = "off"
    return "Light turned off"
