# autoscaler.py
import logging
import os
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from kubernetes import client, config, watch
from kubernetes.client import V2HorizontalPodAutoscaler, V2MetricSpec, V2MetricTarget
from kubernetes.client.rest import ApiException
from prometheus_client import Gauge, Histogram, Counter

logger = logging.getLogger("Autoscaler")

#region Prometheus Metrics
CURRENT_REPLICAS = Gauge("hpa_current_replicas", "Current pod replicas", ["hpa"])
DESIRED_REPLICAS = Gauge("hpa_desired_replicas", "Desired pod replicas", ["hpa"])
SCALE_OPERATIONS = Counter("hpa_scale_ops_total", "Scaling operations", ["hpa", "direction"])
SCALE_ERRORS = Counter("hpa_scale_errors_total", "Scaling errors", ["hpa", "error_type"])
LATENCY = Histogram("hpa_latency_seconds", "API call latency", ["operation"])
#endregion

@dataclass
class ScalingPolicy:
    min_replicas: int = 1
    max_replicas: int = 10
    scale_up_cooldown: int = 300  # seconds
    scale_down_cooldown: int = 600
    target_utilization: float = 0.7
    stabilization_window: int = 300

class HpaAutoscaler:
    """Enterprise-grade Kubernetes HPA controller with custom metrics support"""
    
    def __init__(
        self,
        hpa_name: str,
        namespace: str = "default",
        policy: ScalingPolicy = ScalingPolicy(),
        kubeconfig: Optional[str] = None
    ):
        self.hpa_name = hpa_name
        self.namespace = namespace
        self.policy = policy
        self.last_scale_time = 0.0
        
        # Kubernetes API clients
        self._load_k8s_config(kubeconfig)
        self.autoscaling_api = client.AutoscalingV2Api()
        self.core_api = client.CoreV1Api()
        
        # State tracking
        self.current_replicas = 0
        self.last_metrics: Dict[str, float] = {}
        
        # Thread safety
        self._lock = threading.RLock()
    
    def _load_k8s_config(self, kubeconfig: Optional[str]) -> None:
        """Load cluster configuration safely"""
        try:
            if kubeconfig:
                config.load_kube_config(kubeconfig)
            else:
                config.load_incluster_config()
        except config.ConfigException as e:
            logger.critical("Kubernetes config error: %s", str(e))
            raise
    
    def get_current_state(self) -> Tuple[int, Dict[str, float]]:
        """Fetch current HPA status and metrics"""
        with self._lock, LATENCY.time("read_hpa"):
            try:
                hpa = self.autoscaling_api.read_namespaced_horizontal_pod_autoscaler(
                    name=self.hpa_name,
                    namespace=self.namespace
                )
                
                # Track replicas
                self.current_replicas = hpa.status.current_replicas or 0
                CURRENT_REPLICAS.labels(self.hpa_name).set(self.current_replicas)
                DESIRED_REPLICAS.labels(self.hpa_name).set(hpa.status.desired_replicas)
                
                # Extract metrics
                metrics = {}
                if hpa.status.current_metrics:
                    for metric in hpa.status.current_metrics:
                        key = f"{metric.type.lower()}/{metric.resource.name}"
                        metrics[key] = metric.resource.current.average_utilization
                self.last_metrics = metrics
                
                return self.current_replicas, metrics
                
            except ApiException as e:
                SCALE_ERRORS.labels(self.hpa_name, "api_error").inc()
                logger.error("HPA fetch failed: %s", str(e))
                raise
    
    def adjust_replicas(self, custom_metrics: Dict[str, float]) -> None:
        """Apply scaling decision with cooldown checks"""
        with self._lock:
            current_time = time.time()
            if (current_time - self.last_scale_time) < self._get_cooldown_period():
                logger.debug("Within cooldown period")
                return
                
            try:
                # Calculate desired replicas
                desired = self._calculate_desired_replicas(custom_metrics)
                desired = max(min(desired, self.policy.max_replicas), self.policy.min_replicas)
                
                if desired != self.current_replicas:
                    self._patch_hpa_replicas(desired)
                    self.last_scale_time = current_time
                    direction = "up" if desired > self.current_replicas else "down"
                    SCALE_OPERATIONS.labels(self.hpa_name, direction).inc()
                    
            except Exception as e:
                SCALE_ERRORS.labels(self.hpa_name, "calculation_error").inc()
                logger.error("Scaling calculation failed: %s", str(e))
    
    def _calculate_desired_replicas(self, custom_metrics: Dict[str, float]) -> int:
        """Business logic for scaling decisions"""
        # Combine Kubernetes metrics with custom values
        all_metrics = {**self.last_metrics, **custom_metrics}
        
        # Example: Weighted average of CPU and custom queue length
        cpu_weight = 0.6
        queue_weight = 0.4
        
        cpu_util = all_metrics.get("resource/cpu", 0) 
        queue_util = all_metrics.get("custom/queue_length", 0)
        
        combined = (cpu_util * cpu_weight) + (queue_util * queue_weight)
        return int(self.current_replicas * (combined / self.policy.target_utilization))
    
    def _patch_hpa_replicas(self, desired: int) -> None:
        """Safely update HPA configuration"""
        with LATENCY.time("patch_hpa"):
            try:
                patch = {
                    "spec": {
                        "minReplicas": self.policy.min_replicas,
                        "maxReplicas": self.policy.max_replicas,
                        "metrics": self._build_metric_specs()
                    }
                }
                
                # Atomic update
                self.autoscaling_api.patch_namespaced_horizontal_pod_autoscaler(
                    name=self.hpa_name,
                    namespace=self.namespace,
                    body=patch
                )
                logger.info("Scaled %s from %d to %d", self.hpa_name, self.current_replicas, desired)
                
            except ApiException as e:
                if e.status == 409:
                    logger.warning("Conflict during HPA update, retrying...")
                    self.get_current_state()  # Refresh state
                raise
    
    def _build_metric_specs(self) -> list[V2MetricSpec]:
        """Generate metrics specification for HPA"""
        return [
            V2MetricSpec(
                type="Resource",
                resource=V2MetricTarget(
                    name="cpu",
                    type="Utilization",
                    average_utilization=int(self.policy.target_utilization * 100)
                )
            ),
            # Add custom metrics here
            V2MetricSpec(
                type="External",
                external=V2MetricTarget(
                    metric={
                        "name": "custom/queue_length",
                        "selector": {"matchLabels": {"app": "azeerc-ai"}}
                    },
                    target={"type": "Value", "value": "100"}
                )
            )
        ]
    
    def _get_cooldown_period(self) -> int:
        """Determine cooldown period based on last action"""
        if self.current_replicas == 0:
            return 0
        return self.policy.scale_up_cooldown if self.current_replicas < self.policy.max_replicas \
            else self.policy.scale_down_cooldown

# Example Usage
if __name__ == "__main__":
    import threading
    
    logging.basicConfig(level=logging.INFO)
    
    def metric_collector(autoscaler: HpaAutoscaler):
        """Simulate custom metric updates"""
        while True:
            time.sleep(30)
            autoscaler.adjust_replicas({"custom/queue_length": 150})
    
    scaler = HpaAutoscaler(
        hpa_name="azeerc-ai-scaler",
        policy=ScalingPolicy(
            min_replicas=2,
            max_replicas=20,
            target_utilization=0.65
        )
    )
    
    # Start background updater
    threading.Thread(target=metric_collector, args=(scaler,), daemon=True).start()
    
    # Main watch loop
    w = watch.Watch()
    for event in w.stream(scaler.autoscaling_api.list_namespaced_horizontal_pod_autoscaler, 
                         namespace="default"):
        logger.debug("HPA event: %s", event['type'])
        scaler.get_current_state()
