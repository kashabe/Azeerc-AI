# reward_calculator.py
from __future__ import annotations
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, ValidationError, validator

# Prometheus integration
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger("RewardCalculator")

#region Metrics
REWARD_TOTAL = Counter("reward_calculations_total", "Total reward computations")
REWARD_VALUES = Histogram("reward_values", "Computed reward distribution", buckets=(-np.inf, -10, -1, 0, 1, 10, np.inf))
SLA_VIOLATIONS = Counter("sla_violations", "SLA violation count by type", ["sla_type"])
LATENCY_PERCENTILE = Gauge("latency_percentile", "95th percentile latency (ms)")
#endregion

class SLAConfig(BaseModel):
    """Defines SLA parameters with validation"""
    latency_ms: Tuple[float, float] = (100, 500)  # (target, max)
    throughput_rps: Tuple[float, float] = (1000, 500)
    error_rate: Tuple[float, float] = (0.01, 0.05)
    availability: Tuple[float, float] = (0.999, 0.99)
    priority_weights: Dict[str, float] = {
        "latency": 0.4,
        "throughput": 0.3,
        "error_rate": 0.2,
        "availability": 0.1
    }

    @validator("latency_ms", "throughput_rps", "error_rate", "availability")
    def validate_ranges(cls, v):
        if v[0] >= v[1]:
            raise ValueError("Target must be < max")
        return v

    @validator("priority_weights")
    def validate_weights(cls, v):
        if not math.isclose(sum(v.values()), 1.0, abs_tol=0.001):
            raise ValueError("Weights must sum to 1.0")
        return v

@dataclass(frozen=True)
class SystemState:
    timestamp: datetime
    latency_ms: float
    throughput_rps: float
    error_rate: float
    availability: float
    resource_usage: Dict[str, float]  # {"cpu": 0.7, "memory": 0.65}

class RewardCalculator:
    """Enterprise-grade SLA-driven reward computation with adaptive scaling"""
    
    def __init__(self, config: SLAConfig, warmup_period: int = 300):
        self.config = config
        self.historical_data = pd.DataFrame(columns=[
            'latency', 'throughput', 'errors', 'availability'
        ])
        self.warmup_period = warmup_period
        self._init_adaptive_params()

    def _init_adaptive_params(self):
        """Initialize dynamic scaling factors based on SLA ranges"""
        self.scaling_factors = {
            "latency": 1.0 / (self.config.latency_ms[1] - self.config.latency_ms[0]),
            "throughput": 1.0 / (self.config.throughput_rps[1] - self.config.throughput_rps[0]),
            "error_rate": 1.0 / (self.config.error_rate[1] - self.config.error_rate[0]),
            "availability": 1.0 / (self.config.availability[1] - self.config.availability[0])
        }

    def compute_reward(self, current_state: SystemState) -> float:
        """Calculate composite reward with SLA adherence weighting"""
        REWARD_TOTAL.inc()
        
        # Phase 1: Base SLA component rewards
        latency_reward = self._latency_reward(current_state.latency_ms)
        throughput_reward = self._throughput_reward(current_state.throughput_rps)
        error_reward = self._error_rate_reward(current_state.error_rate)
        availability_reward = self._availability_reward(current_state.availability)

        # Phase 2: Resource efficiency penalty
        resource_penalty = self._resource_penalty(current_state.resource_usage)

        # Phase 3: Adaptive dynamic weighting
        weights = self._calculate_dynamic_weights()

        # Composite reward calculation
        composite = (
            weights["latency"] * latency_reward +
            weights["throughput"] * throughput_reward +
            weights["error_rate"] * error_reward +
            weights["availability"] * availability_reward +
            resource_penalty
        )

        # Record metrics
        REWARD_VALUES.observe(composite)
        LATENCY_PERCENTILE.set(np.percentile(
            self.historical_data['latency'].values, 95
        ) if not self.historical_data.empty else 0)
        
        return round(composite, 4)

    def _latency_reward(self, latency: float) -> float:
        """Non-linear reward for latency SLA"""
        target, max_latency = self.config.latency_ms
        normalized = (latency - target) / (max_latency - target)
        
        if latency <= target:
            return 1.0
        elif latency > max_latency:
            SLA_VIOLATIONS.labels(sla_type="latency").inc()
            return -2.0
        else:
            return 1.0 - math.pow(normalized, 1.5)

    def _throughput_reward(self, throughput: float) -> float:
        """Piecewise reward for throughput SLA"""
        target, min_throughput = self.config.throughput_rps
        
        if throughput >= target:
            return 1.0
        elif throughput < min_throughput:
            SLA_VIOLATIONS.labels(sla_type="throughput").inc()
            return -1.5
        else:
            return (throughput - min_throughput) / (target - min_throughput)

    def _error_rate_reward(self, error_rate: float) -> float:
        """Exponential penalty for error rate"""
        target, max_error = self.config.error_rate
        
        if error_rate <= target:
            return 1.0
        elif error_rate > max_error:
            SLA_VIOLATIONS.labels(sla_type="error_rate").inc()
            return -3.0
        else:
            return 1.0 - math.exp(5 * (error_rate - target))

    def _availability_reward(self, availability: float) -> float:
        """Step function reward for availability"""
        target, min_avail = self.config.availability
        
        if availability >= target:
            return 1.0
        elif availability < min_avail:
            SLA_VIOLATIONS.labels(sla_type="availability").inc()
            return -2.5
        else:
            return 0.5

    def _resource_penalty(self, usage: Dict[str, float]) -> float:
        """Multi-resource utilization penalty"""
        penalty = 0.0
        for resource, util in usage.items():
            if util > 0.8:
                penalty -= 0.5 * (util - 0.8) * 10
            elif util < 0.2:
                penalty -= 0.2 * (0.2 - util) * 5
        return penalty

    def _calculate_dynamic_weights(self) -> Dict[str, float]:
        """Adjust weights based on historical performance"""
        if self.historical_data.shape[0] < self.warmup_period:
            return self.config.priority_weights
        
        recent_data = self.historical_data.tail(100)
        weights = self.config.priority_weights.copy()
        
        # Increase weight for dimensions approaching SLA limits
        for metric in weights:
            std_dev = recent_data[metric].std()
            if std_dev > 0:
                violation_risk = (recent_data[metric].mean() - 
                                 self.config.__getattribute__(metric)[0]) / std_dev
                weights[metric] *= 1 + math.tanh(violation_risk)
        
        # Normalize weights
        total = sum(weights.values())
        return {k: v/total for k, v in weights.items()}

    def update_historical(self, state: SystemState):
        """Maintain rolling window of system states"""
        new_row = {
            'timestamp': state.timestamp,
            'latency': state.latency_ms,
            'throughput': state.throughput_rps,
            'errors': state.error_rate,
            'availability': state.availability
        }
        self.historical_data = pd.concat([
            self.historical_data,
            pd.DataFrame([new_row])
        ], ignore_index=True).tail(1000)  # Keep last 1000 samples

# Example Usage
if __name__ == "__main__":
    from datetime import datetime, timedelta
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize with default SLA config
    sla_config = SLAConfig()
    calculator = RewardCalculator(sla_config)
    
    # Simulate system states
    for i in range(5):
        state = SystemState(
            timestamp=datetime.utcnow() + timedelta(minutes=i),
            latency_ms=np.random.choice([80, 120, 600]),
            throughput_rps=np.random.uniform(800, 1200),
            error_rate=np.random.uniform(0.0, 0.1),
            availability=np.random.uniform(0.98, 1.0),
            resource_usage={"cpu": 0.75, "memory": 0.4}
        )
        calculator.update_historical(state)
        reward = calculator.compute_reward(state)
        print(f"State {i+1}: Reward={reward}")
