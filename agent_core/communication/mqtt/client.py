# client.py
import logging
import random
import ssl
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, Callable, Dict, Optional

import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge

# Metrics
MQTT_CONNECTIONS = Gauge('azeerc_mqtt_active_connections', 'Active MQTT connections')
MQTT_PUBLISHED = Counter('azeerc_mqtt_messages_published', 'Messages published', ['qos', 'topic'])
MQTT_RECEIVED = Counter('azeerc_mqtt_messages_received', 'Messages received', ['qos', 'topic'])

class QoSLevel(IntEnum):
    AT_MOST_ONCE = 0
    AT_LEAST_ONCE = 1
    EXACTLY_ONCE = 2

@dataclass
class MQTTConfig:
    host: str
    port: int = 8883
    client_id: str = f"azeerc-{random.randint(1000, 9999)}"
    tls_ca_certs: Optional[str] = "/etc/ssl/certs/ca-certificates.crt"
    tls_certfile: Optional[str] = None
    tls_keyfile: Optional[str] = None
    clean_start: bool = True
    session_expiry: int = 3600  # Seconds
    max_reconnect_attempts: int = 5

class MQTTClient:
    def __init__(
        self,
        config: MQTTConfig,
        message_callback: Callable[[str, Dict[str, Any]], None],
    ):
        self.config = config
        self._client = mqtt.Client(
            client_id=self.config.client_id,
            protocol=mqtt.MQTTv5,
            transport="tcp"
        )
        self._message_callback = message_callback
        self._pending_publishes: Dict[int, mqtt.MQTTMessage] = {}
        self._connected = False
        self._reconnect_attempts = 0

        # Setup handlers
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.on_publish = self._on_publish

        # Configure TLS
        if self.config.tls_ca_certs:
            self._client.tls_set(
                ca_certs=self.config.tls_ca_certs,
                certfile=self.config.tls_certfile,
                keyfile=self.config.tls_keyfile,
                tls_version=ssl.PROTOCOL_TLSv1_2
            )

    def connect(self) -> None:
        """Establish MQTT connection with QoS 2 support"""
        connect_properties = mqtt.Properties(mqtt.PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = self.config.session_expiry
        
        self._client.connect(
            host=self.config.host,
            port=self.config.port,
            clean_start=self.config.clean_start,
            properties=connect_properties
        )
        self._client.loop_start()

    def disconnect(self) -> None:
        """Graceful disconnection with session preservation"""
        disconnect_properties = mqtt.Properties(mqtt.PacketTypes.DISCONNECT)
        disconnect_properties.SessionExpiryInterval = self.config.session_expiry
        
        self._client.disconnect(properties=disconnect_properties)
        self._client.loop_stop()
        MQTT_CONNECTIONS.dec()

    def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        qos: QoSLevel = QoSLevel.EXACTLY_ONCE,
        retain: bool = False
    ) -> None:
        """Publish message with QoS 2 guarantees"""
        publish_properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        publish_properties.MessageExpiryInterval = 300  # 5 minutes
        
        msg_info = self._client.publish(
            topic=topic,
            payload=json.dumps(payload),
            qos=qos.value,
            retain=retain,
            properties=publish_properties
        )
        
        if qos == QoSLevel.EXACTLY_ONCE:
            self._pending_publishes[msg_info.mid] = msg_info

    def subscribe(self, topic: str, qos: QoSLevel = QoSLevel.EXACTLY_ONCE) -> None:
        """Subscribe with QoS 2 reliability"""
        subscribe_properties = mqtt.Properties(mqtt.PacketTypes.SUBSCRIBE)
        self._client.subscribe(topic, qos=qos.value, properties=subscribe_properties)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Dict[str, int],
        reason_code: int,
        properties: mqtt.Properties
    ) -> None:
        if reason_code == mqtt.ReasonCode(success=0):
            logging.info(f"Connected to {self.config.host}:{self.config.port}")
            self._connected = True
            self._reconnect_attempts = 0
            MQTT_CONNECTIONS.inc()
        else:
            logging.error(f"Connection failed: {reason_code}")
            self._handle_reconnect()

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Dict[str, int],
        reason_code: int,
        properties: mqtt.Properties
    ) -> None:
        logging.warning(f"Disconnected: {reason_code}")
        self._connected = False
        MQTT_CONNECTIONS.dec()
        self._handle_reconnect()

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage
    ) -> None:
        try:
            payload = json.loads(message.payload.decode())
            MQTT_RECEIVED.labels(qos=message.qos, topic=message.topic).inc()
            self._message_callback(message.topic, payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.error(f"Invalid message format: {e}")

    def _on_publish(
        self,
        client: mqtt.Client,
        userdata: Any,
        mid: int
    ) -> None:
        if mid in self._pending_publishes:
            msg_info = self._pending_publishes.pop(mid)
            MQTT_PUBLISHED.labels(qos=msg_info.qos, topic=msg_info.topic).inc()
            logging.debug(f"QoS 2 publish completed: {msg_info.topic}")

    def _handle_reconnect(self) -> None:
        if self._reconnect_attempts < self.config.max_reconnect_attempts:
            delay = min(2 ** self._reconnect_attempts, 30)
            logging.info(f"Reconnecting in {delay}s (attempt {self._reconnect_attempts + 1})")
            time.sleep(delay)
            self._reconnect_attempts += 1
            self.connect()
        else:
            logging.error("Max reconnect attempts reached")

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    def message_handler(topic: str, payload: Dict[str, Any]) -> None:
        logging.info(f"Received message on {topic}: {payload}")

    config = MQTTConfig(
        host="mqtt.azeerc.ai",
        tls_ca_certs=None  # Disable TLS for testing
    )
    
    client = MQTTClient(config, message_handler)
    client.connect()
    
    try:
        client.subscribe("azeerc/agents/#")
        client.publish(
            topic="azeerc/agents/status",
            payload={"agent_id": "123", "status": "active"},
            qos=QoSLevel.EXACTLY_ONCE
        )
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        client.disconnect()
