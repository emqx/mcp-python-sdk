# server.py
from mcp.server.fastmcp import FastMCP

# Create an MCP server
mcp = FastMCP(name = "Demo/a", mqtt_client_id = "abcdeee", debug = True, log_level = "DEBUG")

# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')

