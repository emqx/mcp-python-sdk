# Module-level variable for configurable topic prefix (default: "$mcp" for backward compatibility)
_TOPIC_PREFIX: str = "$mcp"

def set_topic_prefix(prefix: str) -> None:
    """Set the topic prefix for all MCP MQTT topics.
    
    Args:
        prefix: The prefix to use (e.g., "mcp" for AWS IoT, "$mcp" for standard MQTT)
    
    Note: This must be called before creating any MCP transport instances.
    """
    global _TOPIC_PREFIX, SERVER_CONTROL_BASE, SERVER_CAPABILITY_CHANGE_BASE
    global SERVER_PRESENCE_BASE, CLIENT_PRESENCE_BASE, CLIENT_CAPABILITY_CHANGE_BASE, RPC_BASE
    
    _TOPIC_PREFIX = prefix
    SERVER_CONTROL_BASE = f'{prefix}-server'
    SERVER_CAPABILITY_CHANGE_BASE = f'{prefix}-server/capability'
    SERVER_PRESENCE_BASE = f'{prefix}-server/presence'
    CLIENT_PRESENCE_BASE = f'{prefix}-client/presence'
    CLIENT_CAPABILITY_CHANGE_BASE = f'{prefix}-client/capability'
    RPC_BASE = f'{prefix}-rpc'

def get_topic_prefix() -> str:
    """Get the current topic prefix."""
    return _TOPIC_PREFIX

# Topic base constants (can be reconfigured via set_topic_prefix())
SERVER_CONTROL_BASE: str = f'{_TOPIC_PREFIX}-server'
SERVER_CAPABILITY_CHANGE_BASE: str = f'{_TOPIC_PREFIX}-server/capability'
SERVER_PRESENCE_BASE: str = f'{_TOPIC_PREFIX}-server/presence'
CLIENT_PRESENCE_BASE: str = f'{_TOPIC_PREFIX}-client/presence'
CLIENT_CAPABILITY_CHANGE_BASE: str = f'{_TOPIC_PREFIX}-client/capability'
RPC_BASE: str = f'{_TOPIC_PREFIX}-rpc'

def get_server_control_topic(server_id: str, server_name: str) -> str:
    return f"{SERVER_CONTROL_BASE}/{server_id}/{server_name}"

def get_server_capability_change_topic(server_id: str, server_name: str) -> str:
    return f"{SERVER_CAPABILITY_CHANGE_BASE}/{server_id}/{server_name}"

def get_server_presence_topic(server_id: str, server_name: str) -> str:
    return f"{SERVER_PRESENCE_BASE}/{server_id}/{server_name}"

def get_client_presence_topic(mcp_clientid: str) -> str:
    return f"{CLIENT_PRESENCE_BASE}/{mcp_clientid}"

def get_client_capability_change_topic(mcp_clientid: str) -> str:
    return f"{CLIENT_CAPABILITY_CHANGE_BASE}/{mcp_clientid}"

def get_rpc_topic(mcp_clientid: str, server_id: str, server_name: str) -> str:
    return f"{RPC_BASE}/{mcp_clientid}/{server_id}/{server_name}"
