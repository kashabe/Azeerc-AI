# opentelemetry_integration.py
import logging
import os
import re
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Optional

from opentelemetry import context, metrics, trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor

class ObservabilityEngine:
    """Enterprise-grade OpenTelemetry integration with security controls"""
    
    def __init__(self, service_name: str = "azeerc-ai"):
        self.service_name = service_name
        self._configure_resource()
        self._init_tracing()
        self._init_metrics()
        self._setup_auto_instrumentation()
        
    def _configure_resource(self):
        """Configure resource attributes with deployment metadata"""
        self.resource = Resource.create({
            "service.name": self.service_name,
            "service.version": os.getenv("APP_VERSION", "1.0.0"),
            "deployment.environment": os.getenv("ENV", "prod"),
            "cloud.region": os.getenv("AWS_REGION", "us-west-2")
        })
        
    def _init_tracing(self):
        """Initialize distributed tracing with OTLP exporter"""
        self.tracer_provider = TracerProvider(
            resource=self.resource,
            sampler=DynamicSampler()
        )
        self.tracer_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=os.getenv("OTLP_ENDPOINT", "https://collector.azeerc.ai:4317"),
                    insecure=os.getenv("INSECURE_MODE", "false").lower() == "true"
                ),
                max_export_batch_size=500,
                schedule_delay_millis=5000
            )
        )
        trace.set_tracer_provider(self.tracer_provider)
        
    def _init_metrics(self):
        """Configure metrics pipeline with Prometheus compatibility"""
        self.metric_exporter = OTLPMetricExporter(
            endpoint=os.getenv("METRICS_ENDPOINT", "https://metrics.azeerc.ai:4317")
        )
        self.meter_provider = MeterProvider(
            resource=self.resource,
            metric_readers=[
                PeriodicExportingMetricReader(
                    self.metric_exporter,
                    export_interval_millis=60000
                )
            ]
        )
        metrics.set_meter_provider(self.meter_provider)
        
    def _setup_auto_instrumentation(self):
        """Enable automatic instrumentation for common libraries"""
        RequestsInstrumentor().instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider
        )
        
    @contextmanager
    def trace(self, name: str, attributes: Optional[Dict] = None):
        """Context manager for secure tracing operations"""
        tracer = trace.get_tracer(__name__)
        with tracer.start_as_current_span(name, attributes=self._sanitize_attributes(attributes)) as span:
            try:
                yield span
            except Exception as e:
                self._record_error_metric("trace_error")
                span.record_exception(e)
                raise
            
    def _sanitize_attributes(self, attributes: Optional[Dict]) -> Dict:
        """Remove sensitive data from span attributes"""
        if not attributes:
            return {}
            
        sanitized = {}
        sensitive_pattern = re.compile(r"(token|password|key|secret)", re.IGNORECASE)
        for k, v in attributes.items():
            if sensitive_pattern.search(k):
                sanitized[k] = "<REDACTED>"
            else:
                sanitized[k] = str(v)
        return sanitized
        
    def create_counter(self, name: str, description: str = ""):
        """Create managed counter metric with default labels"""
        return self.meter_provider.get_meter(__name__).create_counter(
            name=name,
            description=description,
            unit="1",
            label_keys=["status"]
        )
        
    def record_http_metric(self, metric_name: str, duration: float, status: str):
        """Record HTTP operation metrics with standard dimensions"""
        http_metrics = self.create_counter(metric_name)
        http_metrics.add(1, {"status": status, "method": "GET", "route": "/api"})
        
        latency_histogram = self.meter_provider.get_meter(__name__).create_histogram(
            name=f"{metric_name}.latency",
            unit="ms",
            description=f"Latency distribution of {metric_name}"
        )
        latency_histogram.record(duration, {"status": status})

class DynamicSampler(trace.sampling.Sampler):
    """Enterprise sampling with cost-aware strategy"""
    def get_description(self):
        return "DynamicSampler"
        
    def should_sample(self, parent_context, trace_id, name, attributes, links):
        # Sample all errors and high priority transactions
        if attributes.get("error") or attributes.get("priority") == "high":
            return trace.sampling.Decision(trace.sampling.Decision.RECORD_AND_SAMPLED)
            
        # Sample 20% of other traces
        return trace.sampling.Decision(
            trace.sampling.Decision.RECORD_AND_SAMPLED if hash(trace_id) % 5 == 0 
            else trace.sampling.Decision.DROP
        )

# Enterprise Features

1. **Security Hardening**
```python
def _secure_grpc_channel(self):
    """Configure TLS for OTLP exporters"""
    credentials = grpc.ssl_channel_credentials(
        root_certificates=open('/etc/ssl/certs/ca-certificates.crt').read(),
    )
    return grpc.secure_channel(
        os.getenv("OTLP_ENDPOINT", "collector.azeerc.ai:4317"),
        credentials
    )
