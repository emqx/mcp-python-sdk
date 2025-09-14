SERVER_CONTROL_BASE: str = "$mcp-server"
SERVER_CAPABILITY_CHANGE_BASE: str = "$mcp-server/capability"
SERVER_PRESENCE_BASE: str = "$mcp-server/presence"
CLIENT_PRESENCE_BASE: str = "$mcp-client/presence"
CLIENT_CAPABILITY_CHANGE_BASE: str = "$mcp-client/capability"
RPC_BASE: str = "$mcp-rpc"


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
