# priority_queue.py
from __future__ import annotations
import heapq
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
import uuid

import psycopg2
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, ValidationError

logger = logging.getLogger("PriorityQueue")

#region Prometheus Metrics
QUEUE_SIZE = Gauge("priority_queue_size", "Current queue size by status", ["status"])
TASK_PROCESSING_TIME = Histogram("task_processing_time", "Task handling duration", ["priority"])
TASK_RETRIES = Counter("task_retries_total", "Task retry attempts", ["task_type"])
#endregion

class TaskConfig(BaseModel):
    max_retries: int = 3
    retry_backoff: float = 1.5  # Exponential backoff factor
    timeout: float = 300.0  # Seconds
    dynamic_priority_enabled: bool = True
    db_uri: str = "postgresql://user:pass@localhost/azeerc_tasks"

@dataclass(order=True)
class PrioritizedItem:
    priority: int
    created_at: datetime = field(compare=False)
    adjusted_priority: int = field(compare=False)
    task_id: str = field(compare=False)
    data: Dict[str, Any] = field(compare=False)
    retries: int = 0
    last_attempt: Optional[datetime] = None

class PriorityTaskQueue:
    """Enterprise-grade priority queue with dynamic adjustments and persistence"""
    
    def __init__(self, config: TaskConfig):
        self.config = config
        self._heap: List[PrioritizedItem] = []
        self._lock = threading.RLock()
        self._db_conn = psycopg2.connect(config.db_uri)
        self._init_db()
        self._scheduler_thread = threading.Thread(target=self._monitor_tasks)
        self._scheduler_thread.daemon = True
        self._scheduler_thread.start()

    def _init_db(self):
        """Initialize task persistence table"""
        with self._db_conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    priority INT NOT NULL,
                    adjusted_priority INT NOT NULL,
                    data JSONB NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending', 'processing', 'completed', 'failed')),
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    retries INT NOT NULL,
                    next_attempt TIMESTAMPTZ
                )
            """)
            self._db_conn.commit()

    def _calculate_dynamic_priority(self, item: PrioritizedItem) -> int:
        """Dynamic priority adjustment algorithm"""
        base_priority = item.priority
        if not self.config.dynamic_priority_enabled:
            return base_priority
        
        # Priority boost for aging tasks
        age = (datetime.utcnow() - item.created_at).total_seconds() / 3600
        age_boost = int(age * 0.5)  # +0.5 per hour
        
        # Urgency boost based on retries
        retry_boost = item.retries * 2
        
        return base_priority + age_boost + retry_boost

    def add_task(self, data: Dict[str, Any], priority: int) -> str:
        """Add task with initial priority and persistence"""
        task_id = str(uuid.uuid4())
        created_at = datetime.utcnow()
        item = PrioritizedItem(
            priority=priority,
            created_at=created_at,
            adjusted_priority=priority,
            task_id=task_id,
            data=data
        )
        
        with self._lock, self._db_conn.cursor() as cursor:
            heapq.heappush(self._heap, item)
            cursor.execute("""
                INSERT INTO tasks 
                (task_id, priority, adjusted_priority, data, status, created_at, updated_at, retries)
                VALUES (%s, %s, %s, %s, 'pending', %s, %s, 0)
            """, (
                task_id,
                priority,
                priority,
                psycopg2.extras.Json(data),
                created_at,
                created_at
            ))
            self._db_conn.commit()
        
        QUEUE_SIZE.labels(status='pending').inc()
        return task_id

    def get_next_task(self) -> Optional[Tuple[str, Dict]]:
        """Retrieve highest priority task with locking"""
        with self._lock, self._db_conn.cursor() as cursor:
            if not self._heap:
                self._reload_from_db()
                
            if not self._heap:
                return None

            item = heapq.heappop(self._heap)
            cursor.execute("""
                UPDATE tasks 
                SET status = 'processing', 
                    updated_at = %s,
                    last_attempt = %s
                WHERE task_id = %s
            """, (datetime.utcnow(), datetime.utcnow(), item.task_id))
            self._db_conn.commit()
            
            QUEUE_SIZE.labels(status='pending').dec()
            QUEUE_SIZE.labels(status='processing').inc()
            return item.task_id, item.data

    def complete_task(self, task_id: str):
        """Mark task as successfully completed"""
        with self._lock, self._db_conn.cursor() as cursor:
            cursor.execute("""
                UPDATE tasks 
                SET status = 'completed', 
                    updated_at = %s
                WHERE task_id = %s
            """, (datetime.utcnow(), task_id))
            self._db_conn.commit()
            
            QUEUE_SIZE.labels(status='processing').dec()

    def fail_task(self, task_id: str, error: Optional[Exception] = None):
        """Handle task failure with retry logic"""
        with self._lock, self._db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT retries, data 
                FROM tasks 
                WHERE task_id = %s
            """, (task_id,))
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"Unknown task failed: {task_id}")
                return
                
            retries, data = result
            if retries >= self.config.max_retries:
                cursor.execute("""
                    UPDATE tasks 
                    SET status = 'failed', 
                        updated_at = %s
                    WHERE task_id = %s
                """, (datetime.utcnow(), task_id))
                QUEUE_SIZE.labels(status='processing').dec()
                return
                
            backoff = self.config.retry_backoff ** retries
            next_attempt = datetime.utcnow() + timedelta(seconds=backoff)
            
            cursor.execute("""
                UPDATE tasks 
                SET status = 'pending',
                    retries = retries + 1,
                    updated_at = %s,
                    next_attempt = %s
                WHERE task_id = %s
            """, (datetime.utcnow(), next_attempt, task_id))
            self._db_conn.commit()
            
            # Re-queue with updated priority
            new_item = PrioritizedItem(
                priority=data.get('priority', 100),
                created_at=datetime.utcnow(),
                adjusted_priority=self._calculate_dynamic_priority(
                    PrioritizedItem(**data)
                ),
                task_id=task_id,
                data=data,
                retries=retries+1
            )
            heapq.heappush(self._heap, new_item)
            
            QUEUE_SIZE.labels(status='processing').dec()
            QUEUE_SIZE.labels(status='pending').inc()
            TASK_RETRIES.labels(task_type=data.get('type', 'unknown')).inc()

    def _reload_from_db(self):
        """Reload pending tasks from database"""
        with self._db_conn.cursor() as cursor:
            cursor.execute("""
                SELECT task_id, priority, data, created_at, retries
                FROM tasks
                WHERE status = 'pending'
                ORDER BY adjusted_priority DESC
            """)
            for row in cursor.fetchall():
                item = PrioritizedItem(
                    priority=row[1],
                    created_at=row[3],
                    adjusted_priority=self._calculate_dynamic_priority(
                        PrioritizedItem(priority=row[1], created_at=row[3])
                    ),
                    task_id=row[0],
                    data=row[2],
                    retries=row[4]
                )
                heapq.heappush(self._heap, item)
            QUEUE_SIZE.labels(status='pending').set(cursor.rowcount)

    def _monitor_tasks(self):
        """Background task for priority adjustments and timeouts"""
        while True:
            with self._lock:
                now = datetime.utcnow()
                new_heap = []
                for item in self._heap:
                    # Dynamic priority update
                    item.adjusted_priority = self._calculate_dynamic_priority(item)
                    heapq.heappush(new_heap, item)
                self._heap = new_heap
                
                # Check processing timeouts
                with self._db_conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT task_id 
                        FROM tasks 
                        WHERE status = 'processing' 
                        AND updated_at < %s
                    """, (now - timedelta(seconds=self.config.timeout),))
                    for (task_id,) in cursor.fetchall():
                        logger.warning(f"Task timeout: {task_id}")
                        self.fail_task(task_id)
            
            time.sleep(30)  # Adjust interval as needed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db_conn.close()

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = TaskConfig()
    
    with PriorityTaskQueue(config) as queue:
        # Add sample tasks
        queue.add_task({"type": "urgent", "payload": "data1"}, priority=10)
        queue.add_task({"type": "normal", "payload": "data2"}, priority=100)
        
        # Process tasks
        while True:
            task = queue.get_next_task()
            if not task:
                time.sleep(1)
                continue
                
            task_id, data = task
            start_time = time.monotonic()
            try:
                print(f"Processing {task_id}: {data}")
                # Simulate work
                time.sleep(0.5)
                queue.complete_task(task_id)
            except Exception as e:
                queue.fail_task(task_id, e)
            finally:
                TASK_PROCESSING_TIME.labels(priority=data.get('priority')).observe(
                    time.monotonic() - start_time
                )
