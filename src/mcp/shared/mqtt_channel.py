

SERVICE_CONTROL_BASE: str = '$mcp-service'
SERVICE_CAPABILITY_CHANGE_BASE: str = '$mcp-service/capability-change'
SERVICE_RESOURCE_UPDATE_BASE: str = '$mcp-service/resource-update'
SERVICE_PRESENCE_BASE: str = '$mcp-service/presence'
CLIENT_PRESENCE_BASE: str = '$mcp-client/presence'
CLIENT_CAPABILITY_CHANGE_BASE: str = '$mcp-client/capability-change'
RPC_BASE: str = '$mcp-rpc-endpoint'

def get_service_control_channel(service_name: str) -> str:
    return f"{SERVICE_CONTROL_BASE}/{service_name}"

def get_service_capability_change_channel(service_id: str, service_name: str) -> str:
    return f"{SERVICE_CAPABILITY_CHANGE_BASE}/{service_id}/{service_name}"

def get_service_resource_update_channel(service_id: str, resource_id: str) -> str:
    return f"{SERVICE_RESOURCE_UPDATE_BASE}/{service_id}/{resource_id}"

def get_service_presence_channel(service_id: str, service_name: str) -> str:
    return f"{SERVICE_PRESENCE_BASE}/{service_id}/{service_name}"

def get_client_presence_channel(mcp_clientid: str) -> str:
    return f"{CLIENT_PRESENCE_BASE}/{mcp_clientid}"

def get_client_capability_change_channel(mcp_clientid: str) -> str:
    return f"{CLIENT_CAPABILITY_CHANGE_BASE}/{mcp_clientid}"

def get_rpc_channel(mcp_clientid: str, service_name: str) -> str:
    return f"{RPC_BASE}/{mcp_clientid}/{service_name}"
