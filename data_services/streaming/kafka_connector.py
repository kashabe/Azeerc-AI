# kafka_connector.py
import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger("KafkaConnector")

# Prometheus Metrics
PRODUCED_MESSAGES = Counter("kafka_produced_messages", "Total produced messages", ["topic"])
CONSUMED_MESSAGES = Counter("kafka_consumed_messages", "Total consumed messages", ["topic"])
PRODUCE_ERRORS = Counter("kafka_produce_errors", "Message production failures", ["topic"])
LATENCY_HISTOGRAM = Histogram("kafka_latency_seconds", "End-to-end message latency", ["topic"])
CONSUMER_LAG = Gauge("kafka_consumer_lag", "Consumer group lag", ["topic", "partition"])

@dataclass
class KafkaConfig:
    bootstrap_servers: str
    schema_registry_url: str
    consumer_group: str
    ssl_ca_path: str
    sasl_mechanism: str
    sasl_username: str
    sasl_password: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False

class AsyncKafkaProducer:
    def __init__(self, config: KafkaConfig, serializer: str = "avro"):
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._schema_registry = CachedSchemaRegistryClient({"url": config.schema_registry_url})
        
        self.producer = (AvroProducer({"bootstrap.servers": config.bootstrap_servers,
                                      "security.protocol": "SASL_SSL",
                                      "ssl.ca.location": config.ssl_ca_path,
                                      "sasl.mechanism": config.sasl_mechanism,
                                      "sasl.username": config.sasl_username,
                                      "sasl.password": config.sasl_password},
                                     schema_registry=self._schema_registry)
                        if serializer == "avro" else
                        Producer({"bootstrap.servers": config.bootstrap_servers,
                                 "security.protocol": "SASL_SSL",
                                 "ssl.ca.location": config.ssl_ca_path,
                                 "sasl.mechanism": config.sasl_mechanism,
                                 "sasl.username": config.sasl_username,
                                 "sasl.password": config.sasl_password}))

    async def produce(self, topic: str, value: Dict, key: Optional[str] = None, 
                     schema: Optional[str] = None, retries: int = 3) -> None:
        for attempt in range(retries):
            try:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: self._produce_sync(topic, value, key, schema)
                )
                PRODUCED_MESSAGES.labels(topic=topic).inc()
                return
            except KafkaException as e:
                logger.error(f"Produce attempt {attempt+1} failed: {str(e)}")
                if attempt == retries - 1:
                    PRODUCE_ERRORS.labels(topic=topic).inc()
                    raise
                await asyncio.sleep(2 ** attempt)

    def _produce_sync(self, topic: str, value: Dict, key: str, schema: str):
        if isinstance(self.producer, AvroProducer):
            self.producer.produce(topic=topic, value=value, key=key,
                                value_schema=schema, callback=self._delivery_report)
        else:
            self.producer.produce(topic=topic, value=json.dumps(value), 
                                key=key, callback=self._delivery_report)
        self.producer.poll(0)

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            LATENCY_HISTOGRAM.labels(topic=msg.topic()).observe(time.time() - msg.timestamp()[1])

class AsyncKafkaConsumer:
    def __init__(self, config: KafkaConfig, topics: list[str], 
                process_fn: Callable[[Dict], Awaitable[None]]):
        self.consumer = Consumer({
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": config.consumer_group,
            "auto.offset.reset": config.auto_offset_reset,
            "enable.auto.commit": config.enable_auto_commit,
            "security.protocol": "SASL_SSL",
            "ssl.ca.location": config.ssl_ca_path,
            "sasl.mechanism": config.sasl_mechanism,
            "sasl.username": config.sasl_username,
            "sasl.password": config.sasl_password
        })
        self.topics = topics
        self.process_fn = process_fn
        self._running = False

    async def start(self):
        self._running = True
        self.consumer.subscribe(self.topics)
        while self._running:
            msg = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self.consumer.poll(1.0)
            )
            if msg is None:
                continue
            if msg.error():
                self._handle_error(msg.error())
                continue
            await self._process_message(msg)

    async def _process_message(self, msg):
        try:
            value = json.loads(msg.value()) if msg.value() else {}
            start_time = time.time()
            await self.process_fn(value)
            CONSUMED_MESSAGES.labels(topic=msg.topic()).inc()
            self.consumer.commit(msg)
            LATENCY_HISTOGRAM.labels(topic=msg.topic()).observe(time.time() - start_time)
        except Exception as e:
            logger.error(f"Message processing failed: {str(e)}")
            await self._handle_dlq(msg, str(e))

    def _handle_error(self, error: KafkaError):
        logger.error(f"Consumer error: {error.str()}")
        if error.fatal():
            self.stop()

    async def _handle_dlq(self, msg, error_msg):
        dlq_producer = AsyncKafkaProducer(KafkaConfig(...))
        await dlq_producer.produce(
            topic=f"{msg.topic()}_dlq",
            value={
                "original_message": msg.value(),
                "error": error_msg,
                "timestamp": time.time()
            }
        )

    def stop(self):
        self._running = False
        self.consumer.close()
