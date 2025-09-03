import logging

import anyio

import mcp.client.mqtt as mcp_mqtt
from mcp.shared.mqtt import configure_logging

configure_logging(level="DEBUG")
logger = logging.getLogger(__name__)

async def on_mcp_server_discovered(client, server_name):
    logger.info(f"Discovered {server_name}, connecting ...")
    await client.initialize_mcp_server(server_name)

async def on_mcp_connect(client, server_name, connect_result):
    capabilities = client.get_session(server_name).server_info.capabilities
    logger.info(f"Capabilities of {server_name}: {capabilities}")
    if capabilities.prompts:
        prompts = await client.list_prompts(server_name)
        logger.info(f"Prompts of {server_name}: {prompts}")
    if capabilities.resources:
        resources = await client.list_resources(server_name)
        logger.info(f"Resources of {server_name}: {resources}")
        resource_templates = await client.list_resource_templates(server_name)
        logger.info(f"Resources templates of {server_name}: {resource_templates}")
    if capabilities.tools:
        toolsResult = await client.list_tools(server_name)
        tools = toolsResult.tools
        logger.info(f"Tools of {server_name}: {tools}")

async def on_mcp_disconnect(client, server_name):
    logger.info(f"Disconnected from {server_name}")

async def main():
    async with mcp_mqtt.MqttTransportClient(
        "test_client",
        auto_connect_to_mcp_server = True,
        on_mcp_server_discovered = on_mcp_server_discovered,
        on_mcp_connect = on_mcp_connect,
        on_mcp_disconnect = on_mcp_disconnect,
        mqtt_options = mcp_mqtt.MqttOptions(
            host="broker.emqx.io",
        )
    ) as client:
        client.start()
        while True:
            logger.info("Other works while the MQTT transport client is running in the background...")
            await anyio.sleep(10)

if __name__ == "__main__":
    anyio.run(main)
