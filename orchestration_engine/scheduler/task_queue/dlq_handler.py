# dlq_handler.py
import json
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psycopg2
import requests
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger("DLQHandler")

#region Prometheus Metrics
DLQ_FAILURES = Counter("dlq_failures_total", "Total failed messages", ["source", "error_type"])
DLQ_RETRIES = Counter("dlq_retries_total", "Retry attempts", ["message_type"])
DLQ_PROCESSING_TIME = Histogram("dlq_processing_seconds", "DLQ handling duration")
DLQ_AGE = Gauge("dlq_message_age_seconds", "Oldest message age in DLQ")
#endregion

class DeadLetterConfig(BaseModel):
    max_retries: int = Field(5, ge=1)
    retry_backoff: float = Field(1.8, description="Exponential backoff factor")
    storage_uri: str = "postgresql://dlq_user:pass@localhost/azeerc_dlq"
    alert_webhook: Optional[str] = None
    alert_threshold: int = 3
    cleanup_days: int = 30

class DeadLetterMessage(BaseModel):
    message_id: str
    original_topic: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    failure_reason: str
    stack_trace: Optional[str]
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    retries: int = 0
    last_attempt: Optional[datetime] = None
    permanent_failure: bool = False

class DeadLetterStorage:
    """Persistence layer for DLQ messages"""
    
    def __init__(self, config: DeadLetterConfig):
        self.config = config
        self._conn = psycopg2.connect(config.storage_uri)
        self._init_schema()
        self._lock = threading.RLock()

    def _init_schema(self):
        with self._conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dead_letters (
                    message_id TEXT PRIMARY KEY,
                    original_topic TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    headers JSONB NOT NULL,
                    failure_reason TEXT NOT NULL,
                    stack_trace TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    retries INT NOT NULL,
                    last_attempt TIMESTAMPTZ,
                    permanent_failure BOOLEAN NOT NULL DEFAULT FALSE
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS dlq_retries_idx 
                ON dead_letters (permanent_failure, retries)
            """)
            self._conn.commit()

    def store_message(self, message: DeadLetterMessage):
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dead_letters 
                (message_id, original_topic, payload, headers, failure_reason, 
                 stack_trace, timestamp, retries, last_attempt, permanent_failure)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE SET
                    retries = EXCLUDED.retries,
                    last_attempt = EXCLUDED.last_attempt,
                    permanent_failure = EXCLUDED.permanent_failure
            """, (
                message.message_id,
                message.original_topic,
                json.dumps(message.payload),
                json.dumps(message.headers),
                message.failure_reason,
                message.stack_trace,
                message.timestamp,
                message.retries,
                message.last_attempt,
                message.permanent_failure
            ))
            self._conn.commit()

    def get_retriable_messages(self) -> List[DeadLetterMessage]:
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute("""
                SELECT message_id, original_topic, payload, headers, failure_reason,
                       stack_trace, timestamp, retries, last_attempt, permanent_failure
                FROM dead_letters
                WHERE permanent_failure = FALSE
                  AND retries < %s
                  AND (last_attempt IS NULL OR last_attempt < %s)
                ORDER BY timestamp
                LIMIT 100
            """, (
                self.config.max_retries,
                datetime.utcnow() - timedelta(
                    seconds=self.config.retry_backoff ** self.config.alert_threshold
                )
            ))
            return [DeadLetterMessage(**row) for row in cursor.fetchall()]

    def mark_permanent_failure(self, message_id: str):
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute("""
                UPDATE dead_letters
                SET permanent_failure = TRUE
                WHERE message_id = %s
            """, (message_id,))
            self._conn.commit()

    def cleanup_old_messages(self):
        with self._lock, self._conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM dead_letters
                WHERE timestamp < %s
            """, (datetime.utcnow() - timedelta(days=self.config.cleanup_days),))
            self._conn.commit()

class DeadLetterHandler:
    """Enterprise-grade DLQ processor with retries and alerting"""
    
    def __init__(self, config: DeadLetterConfig):
        self.config = config
        self.storage = DeadLetterStorage(config)
        self._scheduler = threading.Thread(target=self._process_queue)
        self._scheduler.daemon = True
        self._scheduler.start()

    def handle_failure(self, 
                      message: Dict[str, Any], 
                      headers: Dict[str, str], 
                      error: Exception,
                      source_topic: str):
        """Main entry point for failed messages"""
        with DLQ_PROCESSING_TIME.time():
            dl_message = DeadLetterMessage(
                message_id=headers.get("message_id", str(hash(json.dumps(message)))),
                original_topic=source_topic,
                payload=message,
                headers=headers,
                failure_reason=str(error),
                stack_trace=self._get_stack_trace(error)
            )
            
            self.storage.store_message(dl_message)
            DLQ_FAILURES.labels(
                source=source_topic,
                error_type=error.__class__.__name__
            ).inc()
            
            if dl_message.retries >= self.config.alert_threshold:
                self._trigger_alert(dl_message)

    def _process_queue(self):
        """Background thread for retry attempts"""
        while True:
            try:
                messages = self.storage.get_retriable_messages()
                DLQ_AGE.set((datetime.utcnow() - messages[0].timestamp).total_seconds()) if messages else None
                
                for msg in messages:
                    if self._attempt_redelivery(msg):
                        logger.info(f"Successfully redelivered {msg.message_id}")
                        self._delete_message(msg.message_id)
                    else:
                        msg.retries += 1
                        msg.last_attempt = datetime.utcnow()
                        if msg.retries >= self.config.max_retries:
                            msg.permanent_failure = True
                            self._trigger_alert(msg, is_final=True)
                        self.storage.store_message(msg)
                        DLQ_RETRIES.labels(
                            message_type=msg.headers.get("type", "unknown")
                        ).inc()

                self.storage.cleanup_old_messages()
                time.sleep(30)
            except Exception as e:
                logger.error(f"DLQ processing error: {str(e)}")
                time.sleep(60)

    def _attempt_redelivery(self, message: DeadLetterMessage) -> bool:
        """Try resending message to original topic"""
        # Implement your actual broker integration here
        try:
            # Example: Kafka producer retry
            # producer.send(message.original_topic, message.payload, headers=message.headers)
            return True
        except Exception as e:
            logger.warning(f"Redelivery failed for {message.message_id}: {str(e)}")
            return False

    def _trigger_alert(self, message: DeadLetterMessage, is_final: bool = False):
        """Send alerts via configured channels"""
        alert_message = {
            "text": f"{'PERMANENT FAILURE' if is_final else 'WARNING'}: "
                    f"Message {message.message_id} failed {message.retries} times. "
                    f"Last error: {message.failure_reason}",
            "metadata": {
                "message_id": message.message_id,
                "topic": message.original_topic,
                "retries": message.retries,
                "timestamp": message.timestamp.isoformat()
            }
        }
        
        if self.config.alert_webhook:
            try:
                requests.post(
                    self.config.alert_webhook,
                    json=alert_message,
                    timeout=5
                )
            except Exception as e:
                logger.error(f"Alert webhook failed: {str(e)}")
        else:
            logger.warning(alert_message["text"])

    def _get_stack_trace(self, error: Exception) -> str:
        """Extract formatted stack trace"""
        import traceback
        return "".join(traceback.format_exception(
            type(error), error, error.__traceback__
        ))

    def _delete_message(self, message_id: str):
        """Remove successfully processed message"""
        with self.storage._conn.cursor() as cursor:
            cursor.execute("""
                DELETE FROM dead_letters 
                WHERE message_id = %s
            """, (message_id,))
            self.storage._conn.commit()

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = DeadLetterConfig(
        alert_webhook="https://hooks.slack.com/services/TXXXXX/BXXXXX/XXXXXXXX"
    )
    
    handler = DeadLetterHandler(config)
    
    # Simulate a failure
    try:
        raise ValueError("Simulated processing error")
    except Exception as e:
        handler.handle_failure(
            message={"order_id": 123, "data": "test"},
            headers={"message_id": "msg_1", "type": "order"},
            error=e,
            source_topic="orders"
        )
    
    # Keep main thread alive
    while True:
        time.sleep(1)
