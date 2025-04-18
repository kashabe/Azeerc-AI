# executor.py
from __future__ import annotations
import concurrent.futures
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger("TaskExecutor")

#region Prometheus Metrics
TASK_QUEUE_SIZE = Gauge("executor_queue_size", "Pending tasks in queue")
TASK_EXECUTION_TIME = Histogram("executor_task_seconds", "Task runtime distribution", ["task_type"])
TASK_SUCCESS = Counter("executor_tasks_total", "Completed tasks", ["status"])
RESOURCE_UTIL = Gauge("executor_resource_usage", "Resource utilization", ["resource_type"])
#endregion

class TaskStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    TIMEOUT = auto()

@dataclass
class Task:
    task_id: str
    func: Callable[..., Any]
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    priority: int = 0
    retries: int = 3
    timeout: int = 30
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Any] = None
    exception: Optional[Exception] = None

class ExecutionPolicy(Enum):
    FIFO = auto()
    PRIORITY = auto()
    RESOURCE_AWARE = auto()

class Executor:
    """Enterprise-grade parallel task executor with adaptive policies"""
    
    def __init__(
        self,
        max_workers: int = os.cpu_count() or 4,
        policy: ExecutionPolicy = ExecutionPolicy.PRIORITY,
        memory_limit: int = 1024  # MB
    ):
        self._max_workers = max_workers
        self._policy = policy
        self._memory_limit = memory_limit
        self._task_queue = deque()
        self._lock = threading.RLock()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._shutdown_flag = threading.Event()
        self._resource_monitor = threading.Thread(target=self._monitor_resources)
        self._resource_monitor.daemon = True
        self._resource_monitor.start()

    def submit(self, task: Task) -> None:
        """Add task to execution queue with thread-safe operation"""
        with self._lock:
            if self._policy == ExecutionPolicy.PRIORITY:
                # Insert in priority order (lower value = higher priority)
                index = next((i for i, t in enumerate(self._task_queue) if t.priority > task.priority), len(self._task_queue))
                self._task_queue.insert(index, task)
            else:
                self._task_queue.append(task)
            TASK_QUEUE_SIZE.inc()

    def run(self) -> None:
        """Start task processing loop"""
        while not self._shutdown_flag.is_set():
            if self._task_queue:
                with self._lock:
                    task = self._task_queue.popleft()
                    TASK_QUEUE_SIZE.dec()
                
                if self._check_resources():
                    future = self._executor.submit(self._execute_task, task)
                    future.add_done_callback(self._handle_completion)
                else:
                    self._requeue_task(task)
            time.sleep(0.1)

    def _execute_task(self, task: Task) -> Any:
        """Core task execution with monitoring"""
        task.status = TaskStatus.RUNNING
        start_time = time.monotonic()
        
        try:
            with TASK_EXECUTION_TIME.labels(task_type=task.func.__name__).time():
                result = task.func(*task.args, **task.kwargs)
                task.result = result
                task.status = TaskStatus.COMPLETED
                TASK_SUCCESS.labels(status="success").inc()
                return result
        except Exception as e:
            task.exception = e
            task.status = TaskStatus.FAILED
            TASK_SUCCESS.labels(status="failed").inc()
            logger.error(f"Task {task.task_id} failed: {str(e)}")
            if task.retries > 0:
                task.retries -= 1
                self.submit(task)
        finally:
            latency = time.monotonic() - start_time
            if latency > task.timeout:
                task.status = TaskStatus.TIMEOUT
                TASK_SUCCESS.labels(status="timeout").inc()

    def _handle_completion(self, future: concurrent.futures.Future) -> None:
        """Callback for post-execution cleanup"""
        try:
            future.result()
        except Exception as e:
            logger.debug(f"Task completed with exception: {str(e)}")

    def _check_resources(self) -> bool:
        """Enforce memory/CPU constraints"""
        # Implementation varies by platform (psutil for cross-platform)
        current_mem = os.pagesize * os.sysconf('SC_PHYS_PAGES')  # Simplified Linux example
        return (current_mem / (1024 ** 2)) < self._memory_limit

    def _requeue_task(self, task: Task) -> None:
        """Handle resource backpressure"""
        with self._lock:
            self._task_queue.appendleft(task)
            TASK_QUEUE_SIZE.inc()

    def _monitor_resources(self) -> None:
        """Continuous resource tracking"""
        while not self._shutdown_flag.is_set():
            # Actual implementation requires platform-specific metrics
            RESOURCE_UTIL.labels(resource_type="memory").set(0.6)  # Mock value
            RESOURCE_UTIL.labels(resource_type="cpu").set(0.75)   # Mock value
            time.sleep(5)

    def graceful_shutdown(self) -> None:
        """Terminate executor after completing current tasks"""
        self._shutdown_flag.set()
        self._executor.shutdown(wait=True)
        self._resource_monitor.join()

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    def sample_task(x: int) -> int:
        time.sleep(1)
        return x ** 2
    
    executor = Executor(max_workers=2)
    for i in range(5):
        task = Task(
            task_id=f"task_{i}",
            func=sample_task,
            args=(i,),
            priority=i % 2,
            retries=2
        )
        executor.submit(task)
    
    try:
        executor.run()
    except KeyboardInterrupt:
        executor.graceful_shutdown()
