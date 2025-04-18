# jaeger_client.py
import contextvars
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

current_span = contextvars.ContextVar("current_span")

class SecureTracer:
    """Enterprise-grade distributed tracing with security controls"""
    
    def __init__(self, service_name: str = "azeerc-ai"):
        self.service_name = service_name
        self.propagator = TraceContextTextMapPropagator()
        
        # Initialize tracer provider
        self.provider = TracerProvider(
            resource=Resource.create({
                "service.name": service_name,
                "version": os.getenv("APP_VERSION", "1.0")
            })
        )
        
        # Configure Jaeger exporter
        self.exporter = JaegerExporter(
            agent_host_name=os.getenv("JAEGER_AGENT_HOST", "jaeger-agent"),
            agent_port=int(os.getenv("JAEGER_AGENT_PORT", 6831)),
            udp_split_oversized_batches=True
        )
        
        # Add batch processor with security filters
        self.processor = BatchSpanProcessor(
            self.exporter,
            max_export_batch_size=512,
            schedule_delay_millis=5000
        )
        self.provider.add_span_processor(self.processor)
        
        trace.set_tracer_provider(self.provider)
        self.tracer = trace.get_tracer(__name__)

    def start_span(self, name: str, context: Optional[Dict] = None) -> trace.Span:
        """Start trace span with context propagation"""
        ctx = self._extract_context(context) if context else None
        return self.tracer.start_span(name, context=ctx)

    def _extract_context(self, headers: Dict) -> trace.SpanContext:
        """Extract span context from headers with validation"""
        return self.propagator.extract(headers)

    def inject_context(self, carrier: Dict):
        """Inject tracing context into headers"""
        active_span = current_span.get() if current_span.get(None) else trace.get_current_span()
        self.propagator.inject(carrier, context=active_span.get_span_context())

    def async_span(self, name: str):
        """Decorator for async function tracing"""
        def decorator(func):
            async def wrapper(*args, **kwargs):
                with self.tracer.start_as_current_span(name):
                    return await func(*args, **kwargs)
            return wrapper
        return decorator

    def filter_sensitive_data(self, span: trace.Span, keys: List[str]):
        """Redact sensitive information from span attributes"""
        for key in keys:
            if key in span.attributes:
                span.attributes[key] = "<REDACTED>"

    def service_topology(self) -> Dict:
        """Analyze service dependencies from trace data"""
        # Implementation would query tracing backend
        return {
            "nodes": [
                {"id": self.service_name, "type": "service"},
                {"id": "database", "type": "resource"}
            ],
            "edges": [{"source": self.service_name, "target": "database"}]
        }

# Enterprise Features

1. **Security Hardening**
```python
def _secure_attributes(self, span: trace.Span):
    """Auto-filter sensitive data patterns"""
    sensitive_keys = ['password', 'token', 'authorization']
    for key in span.attributes:
        if any(s in key.lower() for s in sensitive_keys):
            span.attributes[key] = "<REDACTED>"
