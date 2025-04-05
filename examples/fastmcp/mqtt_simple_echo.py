"""
FastMCP Echo Server
"""

from mcp.server.fastmcp import FastMCP

# Create server
mcp = FastMCP(
    "demo_server/echo",
    log_level="DEBUG",
    mqtt_server_description="A simple FastMCP server that echoes back the input text.",
    mqtt_options={
        "host": "broker.emqx.io",
    },
)

@mcp.tool()
def echo(text: str) -> str:
    """Echo the input text"""
    return text
