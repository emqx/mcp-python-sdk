import logging
import anyio
import mcp.client.mqtt as mcp_mqtt
from mcp.shared.mqtt import configure_logging

configure_logging(level="DEBUG")
logger = logging.getLogger(__name__)

async def on_mcp_server_presence(client, service_name, status):
    if status == "online":
        logger.info(f"Connecting to {service_name}...")
        await client.initialize_mcp_server(service_name)

async def on_mcp_connect(client, service_name, connect_result):
    logger.info(f"Connect result to {service_name}: {connect_result}")
    capabilities = client.service_sessions[service_name].server_info.capabilities
    logger.info(f"Capabilities of {service_name}: {capabilities}")
    if capabilities.prompts:
        prompts = await client.list_prompts(service_name)
        logger.info(f"Prompts of {service_name}: {prompts}")
    if capabilities.resources:
        resources = await client.list_resources(service_name)
        logger.info(f"Resources of {service_name}: {resources}")
        resource_templates = await client.list_resource_templates(service_name)
        logger.info(f"Resources templates of {service_name}: {resource_templates}")
    if capabilities.tools:
        tools = await client.list_tools(service_name)
        logger.info(f"Tools of {service_name}: {tools}")

async def on_mcp_disconnect(client, service_name, reason):
    logger.info(f"Disconnected from {service_name}, reason: {reason}")
    logger.info(f"Services now: {client.service_sessions}")

async def main():
    async with mcp_mqtt.MqttTransportClient(
        "test_client",
        auto_connect_to_mcp_server = True,
        on_mcp_server_presence = on_mcp_server_presence,
        on_mcp_connect = on_mcp_connect,
        on_mcp_disconnect = on_mcp_disconnect,
        mqtt_options = mcp_mqtt.MqttOptions(
            host="broker.emqx.io",
            port=1883,
            keepalive=60
        )
    ) as client:
        client.start()
        while True:
            logger.info("Other works while the MQTT transport client is running in the background...")
            await anyio.sleep(10)

if __name__ == "__main__":
    anyio.run(main)
