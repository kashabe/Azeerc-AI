# prometheus_exporter.py
import logging
import re
import threading
import time
from collections import defaultdict
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Summary,
    generate_latest,
    start_http_server,
)
from prometheus_client.metrics import MetricWrapperBase

logger = logging.getLogger(__name__)

class InvalidMetricNameError(ValueError):
    """Raised when metric name violates naming conventions"""

class DuplicateMetricError(ValueError):
    """Raised when registering existing metric with different type"""

class PrometheusExporter:
    def __init__(
        self,
        port: int = 8000,
        app_name: str = "azeerc_ai",
        default_labels: Optional[Dict[str, str]] = None,
    ):
        self.port = port
        self.app_name = app_name
        self.default_labels = default_labels or {}
        self._metrics: Dict[str, MetricWrapperBase] = {}
        self._lock = threading.RLock()
        self._registry = CollectorRegistry(auto_describe=True)
        self._http_server_thread: Optional[threading.Thread] = None
        self._setup_core_metrics()

    def _setup_core_metrics(self):
        """Initialize essential system metrics"""
        self.register_metric(
            "process_cpu_seconds_total",
            "Total CPU time spent in seconds",
            "counter",
        )
        self.register_metric(
            "process_memory_bytes",
            "Current memory usage in bytes",
            "gauge",
        )

    @staticmethod
    def _validate_name(name: str) -> bool:
        """Enforce Prometheus metric naming conventions"""
        pattern = r"^[a-zA-Z_:][a-zA-Z0-9_:]*$"
        return re.fullmatch(pattern, name) is not None

    @lru_cache(maxsize=512)
    def register_metric(
        self,
        name: str,
        documentation: str,
        metric_type: str,
        labelnames: Tuple[str, ...] = (),
        buckets: Optional[List[float]] = None,
        **kwargs,
    ) -> MetricWrapperBase:
        """Thread-safe metric registration with type validation"""
        if not self._validate_name(name):
            raise InvalidMetricNameError(f"Invalid metric name: {name}")

        with self._lock:
            if existing := self._metrics.get(name):
                if not isinstance(existing, self._str_to_class(metric_type)):
                    raise DuplicateMetricError(
                        f"Metric {name} already registered as {type(existing)}"
                    )
                return existing

            metric_class = self._str_to_class(metric_type)
            if metric_class is Histogram:
                metric = metric_class(
                    name,
                    documentation,
                    labelnames=labelnames,
                    buckets=buckets,
                    registry=self._registry,
                    **kwargs,
                )
            else:
                metric = metric_class(
                    name,
                    documentation,
                    labelnames=labelnames,
                    registry=self._registry,
                    **kwargs,
                )

            self._metrics[name] = metric
            return metric

    def _str_to_class(self, metric_type: str) -> Any:
        """Map string type to metric class"""
        type_map = {
            "counter": Counter,
            "gauge": Gauge,
            "histogram": Histogram,
            "summary": Summary,
        }
        if metric_type not in type_map:
            raise ValueError(f"Unsupported metric type: {metric_type}")
        return type_map[metric_type]

    def start_server(self, daemon: bool = True) -> None:
        """Start HTTP exporter in background thread"""
        def run_server():
            start_http_server(self.port, registry=self._registry)

        self._http_server_thread = threading.Thread(
            target=run_server, daemon=daemon
        )
        self._http_server_thread.start()
        logger.info(f"Metrics server started on port {self.port}")

    def stop_server(self) -> None:
        """Gracefully stop metrics server"""
        if self._http_server_thread:
            self._http_server_thread.join(timeout=5)
            logger.info("Metrics server stopped")

    def track_execution_time(self, metric_name: str, labels: Dict[str, str] = None):
        """Decorator to measure function execution time"""
        def decorator(func: Callable):
            metric = self.register_metric(
                metric_name,
                f"Execution time of {func.__name__}",
                "histogram",
                labelnames=tuple(labels.keys()) if labels else (),
                buckets=(0.05, 0.1, 0.5, 1, 5, 10),
            )

            def wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start_time
                metric.labels(**labels).observe(duration) if labels else metric.observe(duration)
                return result

            return wrapper
        return decorator

    def generate_metrics_snapshot(self) -> Dict[str, Any]:
        """Capture current metric values for hot reloading"""
        snapshot = {}
        for name, metric in self._metrics.items():
            snapshot[name] = {
                "type": type(metric).__name__.lower(),
                "value": metric.collect()[0].samples,
                "labels": metric._labelnames,
            }
        return snapshot

    def restore_metrics_snapshot(self, snapshot: Dict[str, Any]) -> None:
        """Restore metrics from previously captured snapshot"""
        with self._lock:
            for name, data in snapshot.items():
                self.register_metric(
                    name,
                    "Restored from snapshot",
                    data["type"],
                    labelnames=data["labels"],
                )

class MetricsMiddleware:
    """FastAPI/Starlette middleware for request metrics"""
    
    def __init__(self, app, exporter: PrometheusExporter):
        self.app = app
        self.exporter = exporter
        self.request_count = exporter.register_metric(
            "http_requests_total",
            "Total HTTP requests",
            "counter",
            labelnames=("method", "path", "status"),
        )
        self.request_duration = exporter.register_metric(
            "http_request_duration_seconds",
            "HTTP request duration",
            "histogram",
            labelnames=("method", "path", "status"),
            buckets=(0.01, 0.1, 0.5, 1, 5, 10),
        )

    async def __call__(self, scope, receive, send):
        start_time = time.perf_counter()
        method = scope.get("method", "")
        path = scope.get("path", "")

        async def wrapped_send(response):
            if response["type"] == "http.response.start":
                status = response["status"]
                duration = time.perf_counter() - start_time
                
                self.request_count.labels(method, path, status).inc()
                self.request_duration.labels(method, path, status).observe(duration)
                
            await send(response)

        await self.app(scope, receive, wrapped_send)

# Example Usage
if __name__ == "__main__":
    exporter = PrometheusExporter(port=8000)
    exporter.start_server()

    # Register custom business metrics
    active_agents = exporter.register_metric(
        "azeerc_ai_active_agents",
        "Currently active AI agents",
        "gauge",
        labelnames=("agent_type", "region"),
    )
    inference_errors = exporter.register_metric(
        "azeerc_ai_inference_errors_total",
        "Total inference processing errors",
        "counter",
        labelnames=("error_code",),
    )

    # Update metrics
    active_agents.labels(agent_type="nlp", region="us-west").inc()
    inference_errors.labels(error_code="500").inc()

    # Keep alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        exporter.stop_server()

# Enterprise Features

1. **Metric Validation**  
```python
assert exporter._validate_name("valid_metric_1")  # Passes
assert not exporter._validate_name("3invalid_metric")  # Fails
