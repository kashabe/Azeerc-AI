# iot_gateway.py
import asyncio
import logging
import ssl
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional

from aiocoap import Context, Message, Code
from aiocoap.resource import Resource
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

@dataclass
class GatewayConfig:
    mqtt_broker: str = "mqtt.azeerc.ai"
    mqtt_port: int = 8883
    coap_port: int = 5684
    tls_cert: str = "/certs/fullchain.pem"
    tls_key: str = "/certs/privkey.pem"
    max_message_size: int = 2048
    qos_level: int = 1
    rate_limit: int = 1000  # Messages/second

class IoTGateway:
    def __init__(self, config: GatewayConfig):
        self.config = config
        self.mqtt_client = mqtt.Client(
            client_id="gateway-bridge",
            transport="tcp",
            protocol=mqtt.MQTTv5
        )
        self.coap_context: Optional[Context] = None
        self.message_queue = asyncio.Queue(maxsize=1000)
        self._setup_security()

    def _setup_security(self):
        """Configure TLS and device authentication"""
        self.tls_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        self.tls_context.load_cert_chain(
            certfile=self.config.tls_cert,
            keyfile=self.config.tls_key
        )
        self.tls_context.set_ciphers("ECDHE-ECDSA-AES256-GCM-SHA384")

    async def start(self):
        """Start both protocol servers"""
        await asyncio.gather(
            self._start_mqtt_client(),
            self._start_coap_server(),
            self._message_router()
        )

    async def _start_mqtt_client(self):
        """Connect to MQTT broker with TLS"""
        self.mqtt_client.tls_set_context(self.tls_context)
        self.mqtt_client.on_message = self._mqtt_message_handler
        self.mqtt_client.connect(
            self.config.mqtt_broker,
            self.config.mqtt_port
        )
        self.mqtt_client.subscribe("#", qos=self.config.qos_level)
        self.mqtt_client.loop_start()

    def _mqtt_message_handler(self, client, userdata, msg):
        """Process incoming MQTT messages"""
        try:
            if msg.topic.startswith("$SYS"):
                return  # Ignore system topics
            
            payload = msg.payload.decode().strip()
            asyncio.create_task(
                self.message_queue.put(("mqtt", msg.topic, payload))
            )
        except Exception as e:
            logger.error(f"MQTT handler error: {str(e)}")

    async def _start_coap_server(self):
        """Initialize CoAP server with DTLS"""
        class GatewayResource(Resource):
            def __init__(self, queue):
                super().__init__()
                self.queue = queue

            async def render_put(self, request):
                try:
                    await self.queue.put(("coap", request.opt.uri_path, request.payload))
                    return Message(code=Code.CHANGED)
                except Exception as e:
                    logger.error(f"CoAP error: {str(e)}")
                    return Message(code=Code.INTERNAL_SERVER_ERROR)

        root = Resource()
        root.add_resource(["iot"], GatewayResource(self.message_queue))
        self.coap_context = await Context.create_server_context(
            root,
            bind=("::", self.config.coap_port),
            ssl_context=self.tls_context
        )

    async def _message_router(self):
        """Route messages between protocols"""
        while True:
            protocol, path, payload = await self.message_queue.get()
            try:
                if protocol == "mqtt":
                    await self._coap_forward(path, payload)
                else:
                    self._mqtt_forward(path, payload)
            except Exception as e:
                logger.error(f"Routing error: {str(e)}")
            finally:
                self.message_queue.task_done()

    async def _coap_forward(self, topic: str, payload: str):
        """Convert MQTT to CoAP message"""
        uri_path = topic.replace("/", ":")
        message = Message(
            code=Code.PUT,
            payload=payload.encode(),
            uri_path=uri_path.split(":")
        )
        await self.coap_context.request(message).response

    def _mqtt_forward(self, path: list, payload: bytes):
        """Convert CoAP to MQTT message"""
        topic = "/".join(path)
        self.mqtt_client.publish(
            topic,
            payload.decode(),
            qos=self.config.qos_level,
            retain=False
        )

    async def stop(self):
        """Graceful shutdown"""
        self.mqtt_client.disconnect()
        await self.coap_context.shutdown()
        await self.message_queue.join()

# Production Deployment
config = GatewayConfig(
    mqtt_broker="iot.azeerc.ai",
    rate_limit=5000,
    tls_cert="/etc/letsencrypt/live/iot.azeerc.ai/fullchain.pem",
    tls_key="/etc/letsencrypt/live/iot.azeerc.ai/privkey.pem"
)

async def main():
    gateway = IoTGateway(config)
    await gateway.start()

# Enterprise Features

1. **Security Hardening**
```python
# Add mutual TLS authentication
config.tls_context.load_verify_locations(cafile="/ca/root.crt")
config.tls_context.verify_mode = ssl.CERT_REQUIRED
