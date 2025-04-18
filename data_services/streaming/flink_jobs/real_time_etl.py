# real_time_etl.py
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any, Tuple

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.execution_config import ExecutionConfig
from pyflink.datastream.functions import ProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.time import Time
from pyflink.util.java_utils import get_j_env_configuration

logger = logging.getLogger("RealtimeETL")

class ETLPipeline:
    def __init__(self, env: StreamExecutionEnvironment):
        self.env = env
        self._configure_environment()

    def _configure_environment(self):
        # Configure production-grade settings
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.enable_checkpointing(5000)  # 5 seconds
        self.env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
        self.env.set_restart_strategy(
            "failure-rate", 
            failure_rate=2, 
            failure_interval=Time.minutes(5),
            delay_interval=Time.seconds(30)
        )
        self.env.add_default_kryo_serializers({
            dict: org.apache.flink.api.common.typeutils.TypeSerializerProxy()
        })

    def build_pipeline(self):
        source = self._create_kafka_source()
        sink = self._create_elasticsearch_sink()
        error_sink = self._create_dlq_sink()

        main_stream = self.env.from_source(
            source,
            WatermarkStrategy.for_monotonous_timestamps(),
            "Kafka Source"
        ).name("kafka-source").uid("kafka-source")

        # Processing pipeline
        processed_stream = (
            main_stream
            .map(self.parse_json, output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTES()))
            .name("json-parser").uid("json-parser")
            .process(FieldValidator(), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTES()))
            .name("field-validator").uid("field-validator")
            .key_by(lambda x: x["device_id"])
            .process(StatefulEnrichment(), output_type=Types.MAP(Types.STRING(), Types.PICKLED_BYTES()))
            .name("state-enricher").uid("state-enricher")
        )

        # Split error stream
        error_stream = processed_stream.get_side_output(self.error_tag)
        processed_stream.add_sink(sink).name("es-sink").uid("es-sink")
        error_stream.add_sink(error_sink).name("dlq-sink").uid("dlq-sink")

    def _create_kafka_source(self):
        return KafkaSource.builder() \
            .set_bootstrap_servers(os.getenv("KAFKA_BROKERS", "kafka:9092")) \
            .set_group_id("azeerc-etl-group") \
            .set_topics("raw-events") \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_properties({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';" % (
                    os.getenv("KAFKA_USER"),
                    os.getenv("KAFKA_PASSWORD")
                )
            }).build()

    def _create_elasticsearch_sink(self):
        from pyflink.datastream.connectors.elasticsearch import ElasticsearchSink
        from pyflink.datastream.connectors.elasticsearch7 import ElasticsearchEmitter

        http_hosts = [{"host": os.getenv("ES_HOST", "elasticsearch"), "port": 9200}]
        
        es_sink = ElasticsearchSink(
            http_hosts=http_hosts,
            emitter=ElasticsearchEmitter(
                index_prefix="azeerc-",
                doc_type="_doc",
                bulk_size=1000,
                tls_verify=False,
                username=os.getenv("ES_USER"),
                password=os.getenv("ES_PASSWORD")
            )
        )
        return es_sink

    def _create_dlq_sink(self):
        return KafkaSink.builder() \
            .set_bootstrap_servers(os.getenv("KAFKA_BROKERS")) \
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                .set_topic("etl-dlq")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            ).build()

class FieldValidator(ProcessFunction):
    def __init__(self):
        super().__init__()
        self.error_tag = None

    def open(self, runtime_context: RuntimeContext):
        self.error_tag = MapStateDescriptor("errors", Types.STRING(), Types.PICKLED_BYTES())

    def process_element(self, value, ctx):
        required_fields = ["event_id", "timestamp", "device_id"]
        if not all(field in value for field in required_fields):
            ctx.output(self.error_tag, {
                "original": value,
                "error": "Missing required fields",
                "timestamp": time.time()
            })
            return None
        return value

class StatefulEnrichment(ProcessFunction):
    def __init__(self):
        self.state = None
        self.cache_state = None

    def open(self, runtime_context: RuntimeContext):
        self.state = runtime_context.get_state(
            ValueStateDescriptor("device_history", Types.PICKLED_BYTES())
        )
        self.cache_state = runtime_context.get_map_state(
            MapStateDescriptor("metadata_cache", Types.STRING(), Types.PICKLED_BYTES())
        )

    def process_element(self, value, ctx):
        # Enrich with historical data
        history = self.state.value() or []
        history.append(value["event_id"])
        if len(history) > 1000:
            history.pop(0)
        self.state.update(history)

        # Enrich with cached metadata
        device_id = value["device_id"]
        if device_id not in self.cache_state:
            self.cache_state.put(device_id, fetch_metadata(device_id))
        
        value["metadata"] = self.cache_state.get(device_id)
        return value

def fetch_metadata(device_id: str) -> Dict[str, Any]:
    # Implement Redis/DB lookup with cache
    return {"location": "unknown", "model": "default"}

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar")

    pipeline = ETLPipeline(env)
    pipeline.build_pipeline()
    env.execute("Azeerc Realtime ETL")
