"""
FastMCP Echo Server
"""

from mcp.server.fastmcp import FastMCP
from mcp.server.mqtt import MqttOptions

rbac_opts = {
    "rbac": {
        "roles": [
            {
                "name": "admin",
                "description": "Administrator role with full access",
                "allowed_methods": [
                    "notifications/initialized",
                    "ping", "tools/list", "tools/call", "resources/list", "resources/read",
                    "resources/subscribe", "resources/unsubscribe"
                ],
                "allowed_tools": "all",
                "allowed_resources": "all"
            },
            {
                "name": "user",
                "description": "User role with limited access",
                "allowed_methods": [
                    "notifications/initialized",
                    "ping", "tools/list", "tools/call", "resources/list", "resources/read"
                ],
                "allowed_tools": [
                    "get_vehicle_status", "get_vehicle_location"
                ],
                "allowed_resources": [
                    "file:///vehicle/telemetry.data"
                ]
            }
        ]
    }
}

# Create server
mcp = FastMCP(
    "demo_server/echo",
    server_version="emqx-demo:1.0.0",
    log_level="DEBUG",
    mqtt_server_description="A simple FastMCP server that echoes back the input text.",
    mqtt_server_meta = rbac_opts,
    mqtt_client_id = "abcdeee",
    mqtt_options=MqttOptions(
        username="aaa",
        host="localhost",
        port=1883,
    ),
)

@mcp.tool()
def echo(text: str) -> str:
    """Echo the input text"""
    return text
