"""
FastMCP Echo Server
"""

from mcp.server.fastmcp import FastMCP

# Create server
mcp = FastMCP(
    "demo_server/echo",
    log_level="DEBUG",
    mqtt_service_description="A simple FastMCP server that echoes back the input text."
)

@mcp.tool()
def echo(text: str) -> str:
    """Echo the input text"""
    return text
