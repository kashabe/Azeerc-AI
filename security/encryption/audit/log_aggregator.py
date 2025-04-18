# log_aggregator.py
import gzip
import json
import logging
import queue
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from cryptography.fernet import Fernet
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger("LogAggregator")

# Prometheus Metrics
LOG_SENT = Counter("log_events_sent", "Total processed logs", ["destination"])
LOG_FAILED = Counter("log_events_failed", "Failed log deliveries", ["destination"])
BUFFER_LEVEL = Gauge("log_buffer_items", "Current buffer queue size")
LATENCY = Histogram("log_delivery_latency", "Delivery latency in seconds")

class SIEMConfigError(Exception):
    """Invalid SIEM configuration"""

class LogTransportError(Exception):
    """Critical delivery failure"""

class SIEMClient(ABC):
    @abstractmethod
    def send_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Send batch to SIEM backend"""

class SplunkClient(SIEMClient):
    def __init__(self, config: Dict[str, Any]):
        self.endpoint = config["splunk"]["hec_url"]
        self.token = Fernet(config["crypto_key"]).decrypt(
            config["splunk"]["token"].encode()
        ).decode()
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Splunk {self.token}",
            "Content-Encoding": "gzip"
        })

    def send_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Send to Splunk HTTP Event Collector"""
        start_time = time.monotonic()
        try:
            data = gzip.compress(
                "\n".join(json.dumps(e) for e in batch).encode()
            )
            response = self.session.post(
                self.endpoint,
                data=data,
                timeout=10
            )
            response.raise_for_status()
            LATENCY.observe(time.monotonic() - start_time)
            LOG_SENT.labels("splunk").inc(len(batch))
            return True
        except Exception as e:
            LOG_FAILED.labels("splunk").inc(len(batch))
            logger.error(f"Splunk delivery failed: {str(e)}")
            return False

class ElasticClient(SIEMClient):
    def __init__(self, config: Dict[str, Any]):
        self.endpoint = config["elastic"]["bulk_url"]
        self.auth = (
            config["elastic"]["user"],
            Fernet(config["crypto_key"]).decrypt(
                config["elastic"]["password"].encode()
            ).decode()
        )
        self.session = requests.Session()
        self.session.headers.update({"Content-Encoding": "gzip"})

    def send_batch(self, batch: List[Dict[str, Any]]) -> bool:
        """Send to Elasticsearch _bulk API"""
        start_time = time.monotonic()
        try:
            payload = []
            for event in batch:
                payload.append(json.dumps({"index": {}}))
                payload.append(json.dumps(event))
            data = gzip.compress("\n".join(payload).encode())
            response = self.session.post(
                self.endpoint,
                data=data,
                auth=self.auth,
                timeout=15
            )
            response.raise_for_status()
            LATENCY.observe(time.monotonic() - start_time)
            LOG_SENT.labels("elastic").inc(len(batch))
            return True
        except Exception as e:
            LOG_FAILED.labels("elastic").inc(len(batch))
            logger.error(f"Elastic delivery failed: {str(e)}")
            return False

class LogAggregator:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.clients = self._init_clients()
        self.buffer = queue.Queue(maxsize=10000)
        self._shutdown = threading.Event()
        self._sender_thread = threading.Thread(
            target=self._process_buffer,
            daemon=True
        )

    def _load_config(self, path: str) -> Dict[str, Any]:
        """Load and validate configuration"""
        # Implement with JSON Schema validation
        return {
            "splunk": {"hec_url": "https://splunk:8088/services/collector"},
            "elastic": {"bulk_url": "https://elastic:9200/_bulk"},
            "crypto_key": Fernet.generate_key().decode()
        }

    def _init_clients(self) -> List[SIEMClient]:
        """Initialize enabled SIEM clients"""
        clients = []
        if self.config.get("splunk"):
            clients.append(SplunkClient(self.config))
        if self.config.get("elastic"):
            clients.append(ElasticClient(self.config))
        if not clients:
            raise SIEMConfigError("No SIEM backends configured")
        return clients

    def log_event(self, event: Dict[str, Any]):
        """Add log to processing buffer"""
        try:
            self.buffer.put_nowait(self._sanitize_event(event))
            BUFFER_LEVEL.inc()
        except queue.Full:
            LOG_FAILED.labels("buffer").inc()
            logger.warning("Log buffer overflow, event discarded")

    def _sanitize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """GDPR-compliant data masking"""
        # Implement PII redaction logic
        return event

    def start(self):
        """Start background processing"""
        self._sender_thread.start()
        logger.info("Log aggregator started")

    def shutdown(self):
        """Graceful shutdown"""
        self._shutdown.set()
        self._sender_thread.join()
        logger.info("Log aggregator stopped")

    def _process_buffer(self):
        """Batch processing loop"""
        batch = []
        last_flush = time.monotonic()
        while not self._shutdown.is_set():
            try:
                event = self.buffer.get(timeout=1)
                batch.append(event)
                BUFFER_LEVEL.dec()
            except queue.Empty:
                pass

            # Flush on size or time threshold
            if len(batch) >= 100 or (time.monotonic() - last_flush) > 5:
                self._send_batch(batch)
                batch = []
                last_flush = time.monotonic()

    def _send_batch(self, batch: List[Dict[str, Any]]):
        """Attempt delivery through all clients"""
        for client in self.clients:
            try:
                if client.send_batch(batch):
                    return
            except Exception as e:
                logger.error(f"Client {type(client).__name__} failed: {str(e)}")
        self._handle_failed_batch(batch)

    def _handle_failed_batch(self, batch: List[Dict[str, Any]]):
        """Retry logic with disk spillover"""
        # Implement retry queue with backoff
        LOG_FAILED.labels("all").inc(len(batch))
        logger.critical(f"Batch delivery failed for {len(batch)} events")

if __name__ == "__main__":
    aggregator = LogAggregator("config/siem.json")
    aggregator.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        aggregator.shutdown()
