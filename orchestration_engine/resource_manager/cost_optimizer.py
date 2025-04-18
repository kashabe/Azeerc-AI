# cost_optimizer.py
import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple

import boto3
from google.cloud import compute_v1
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel

logger = logging.getLogger("CostOptimizer")

#region Prometheus Metrics
COST_SAVINGS = Counter("cloud_cost_savings_total", "Total saved cloud costs", ["provider"])
CURRENT_COST = Gauge("cloud_current_cost", "Current hourly cost estimate", ["provider"])
OPTIMIZATION_LATENCY = Histogram("optimizer_latency_seconds", "Optimization loop duration")
#endregion

class CloudProvider(str, Enum):
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

@dataclass
class InstanceProfile:
    instance_type: str
    price: float  # USD/hour
    spot_price: Optional[float] = None
    vCPUs: int
    memory_gb: int
    available_zones: List[str]

@dataclass
class OptimizationTarget:
    min_performance: float  # vCPU-seconds/hour
    max_budget: float       # USD/hour
    reliability: float     # 0.0-1.0

class CloudAdapter(ABC):
    @abstractmethod
    def get_instance_profiles(self) -> List[InstanceProfile]:
        pass
    
    @abstractmethod
    def get_current_cost(self) -> float:
        pass
    
    @abstractmethod
    def apply_scaling_plan(self, plan: ScalingPlan):
        pass

class AWSOptimizer(CloudAdapter):
    def __init__(self, region: str = "us-west-2"):
        self.ec2 = boto3.client("ec2", region_name=region)
        self.region = region
    
    def get_instance_profiles(self) -> List[InstanceProfile]:
        response = self.ec2.describe_spot_price_history(
            InstanceTypes=["c5.xlarge", "m5.large"],  # Add more types
            ProductDescriptions=["Linux/UNIX"],
            MaxResults=100
        )
        # Process spot prices and merge with On-Demand data
        return self._build_profiles(response)
    
    def apply_scaling_plan(self, plan: ScalingPlan):
        # Implement AWS-specific scaling (ASG/EC2 Fleet)
        pass

class GCPOptimizer(CloudAdapter):
    def __init__(self, project: str, zone: str):
        self.client = compute_v1.InstancesClient()
        self.project = project
        self.zone = zone
    
    def get_instance_profiles(self) -> List[InstanceProfile]:
        # Fetch preemptible VM prices
        pass

class CostOptimizer:
    """Multi-cloud cost optimization engine with performance constraints"""
    
    def __init__(
        self,
        providers: List[CloudAdapter],
        target: OptimizationTarget,
        refresh_interval: int = 300
    ):
        self.providers = providers
        self.target = target
        self.refresh_interval = refresh_interval
        self._running = False
        self._lock = threading.RLock()
        self._scheduler = threading.Thread(target=self._optimization_loop, daemon=True)
    
    def start(self):
        self._running = True
        self._scheduler.start()
        logger.info("Cost optimizer started")
    
    def stop(self):
        self._running = False
        self._scheduler.join()
        logger.info("Cost optimizer stopped")
    
    def _optimization_loop(self):
        while self._running:
            start_time = time.monotonic()
            
            try:
                with OPTIMIZATION_LATENCY.time():
                    current_cost = 0.0
                    for provider in self.providers:
                        profiles = provider.get_instance_profiles()
                        optimal_plan = self._calculate_optimal_plan(profiles)
                        provider.apply_scaling_plan(optimal_plan)
                        
                        cost = provider.get_current_cost()
                        current_cost += cost
                        CURRENT_COST.labels(provider.__class__.__name__).set(cost)
                    
                    logger.info(f"Optimization cycle completed in {time.monotonic()-start_time:.2f}s")
                
                sleep_time = self.refresh_interval - (time.monotonic() - start_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)
            
            except Exception as e:
                logger.error(f"Optimization failed: {str(e)}")
                time.sleep(30)  # Backoff
    
    def _calculate_optimal_plan(self, profiles: List[InstanceProfile]) -> ScalingPlan:
        """Mixed Integer Programming for cost/performance balance"""
        # Simplified greedy algorithm for demonstration
        sorted_instances = sorted(profiles, key=lambda x: x.price/x.vCPUs)
        
        selected = []
        total_vcpu = 0
        total_cost = 0
        
        for instance in sorted_instances:
            if total_cost + instance.price > self.target.max_budget:
                break
            
            if instance.spot_price and (self.target.reliability < 0.9):
                price = instance.spot_price
            else:
                price = instance.price
            
            max_count = math.floor((self.target.max_budget - total_cost) / price)
            if max_count == 0:
                continue
            
            selected.append((instance, max_count))
            total_cost += price * max_count
            total_vcpu += instance.vCPUs * max_count
        
        if total_vcpu < self.target.min_performance:
            logger.warning("Cannot meet performance target within budget")
        
        return ScalingPlan(instances=selected)

@dataclass
class ScalingPlan:
    instances: List[Tuple[InstanceProfile, int]]  # (instance_type, count)
    schedule: Optional[datetime] = None          # For time-based scaling

# Example Usage
if __name__ == "__main__":
    import os
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize cloud adapters
    aws_adapter = AWSOptimizer(region="us-west-2")
    gcp_adapter = GCPOptimizer(project=os.getenv("GCP_PROJECT"), zone="us-central1-a")
    
    optimizer = CostOptimizer(
        providers=[aws_adapter, gcp_adapter],
        target=OptimizationTarget(
            min_performance=100000,  # 100k vCPU-seconds/hour
            max_budget=50.0,         # \$50/hour
            reliability=0.95
        ),
        refresh_interval=300
    )
    
    try:
        optimizer.start()
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        optimizer.stop()
