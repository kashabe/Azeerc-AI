# policy_manager.py
from __future__ import annotations
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type
from uuid import UUID, uuid4

import ray
import yaml
from ray import rllib
from ray.rllib.algorithms import Algorithm, AlgorithmConfig
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.typing import PolicyID
from pydantic import BaseModel, validate_arguments

logger = logging.getLogger("PolicyManager")

@dataclass(frozen=True)
class PolicySpec:
    policy_id: UUID
    policy_class: Type[Policy]
    config: AlgorithmConfig
    version: str
    metadata: Dict[str, Any]
    is_active: bool = True

class PolicyConfig(BaseModel):
    algorithm: str
    hyperparameters: Dict[str, Any]
    observation_space: Dict[str, Any]
    action_space: Dict[str, Any]
    version_constraints: Dict[str, str]

class PolicyManager:
    """Orchestrates multi-agent policies with version control and A/B testing"""
    
    def __init__(self, ray_address: Optional[str] = None):
        self._init_ray(ray_address)
        self.policy_registry: Dict[UUID, PolicySpec] = {}
        self.active_policies: Dict[str, List[PolicySpec]] = defaultdict(list)
        self.algorithm_cache: Dict[UUID, Algorithm] = {}
        self.metrics_client = MetricsClient()  # Prometheus/Grafana integration
        
        # Load base configuration
        self._load_base_config()
        
        logger.info(f"PolicyManager initialized with Ray@{ray_address or 'local'}")

    @validate_arguments
    def register_policy(
        self,
        policy_class: Type[Policy],
        config: PolicyConfig,
        metadata: Optional[Dict[str, Any]] = None
    ) -> UUID:
        """Register new policy version with validation"""
        policy_id = uuid4()
        spec = PolicySpec(
            policy_id=policy_id,
            policy_class=policy_class,
            config=self._build_algorithm_config(config),
            version=config.version_constraints.get("min_version", "1.0.0"),
            metadata=metadata or {},
        )
        
        self.policy_registry[policy_id] = spec
        self.active_policies[spec.metadata.get("domain", "default")].append(spec)
        self.metrics_client.inc("policies_registered_total")
        
        logger.info(f"Registered policy {policy_id} v{spec.version}")
        return policy_id

    def get_policy(self, policy_id: UUID) -> Algorithm:
        """Get or instantiate cached policy algorithm"""
        if policy_id not in self.algorithm_cache:
            spec = self.policy_registry[policy_id]
            algo = spec.config.build()
            self.algorithm_cache[policy_id] = algo
        return self.algorithm_cache[policy_id]

    @validate_arguments
    def compute_actions(
        self,
        policy_id: UUID,
        observation: Dict[str, Any],
        explore: bool = True
    ) -> Tuple[Any, List[Any], Dict[str, Any]]:
        """Unified inference interface with metrics"""
        start_time = time.monotonic()
        try:
            algo = self.get_policy(policy_id)
            actions, states, infos = algo.compute_single_action(
                observation,
                explore=explore,
                policy_id=str(policy_id)
            )
            self.metrics_client.timing("inference_latency_seconds", time.monotonic() - start_time)
            return actions, states, infos
        except Exception as e:
            self.metrics_client.inc("inference_errors_total")
            logger.error(f"Inference failed: {str(e)}")
            raise

    def rollout_worker(self, env_config: Dict[str, Any]) -> Dict[str, Any]:
        """Distributed policy evaluation worker"""
        env = AzeercMultiAgentEnv(config=EnvironmentConfig(**env_config))
        return self._run_episode(env)

    def train_iteration(self, policy_ids: List[UUID]) -> Dict[str, Any]:
        """Coordinated multi-policy training"""
        results = {}
        for pid in policy_ids:
            algo = self.get_policy(pid)
            result = algo.train()
            results[str(pid)] = result
            self._sync_policy_weights(pid)
        return results

    def export_policy(self, policy_id: UUID, export_format: str = "onnx") -> bytes:
        """Model export for production serving"""
        algo = self.get_policy(policy_id)
        return algo.export_policy_model(
            policy_id=str(policy_id),
            export_format=export_format
        )

    def _init_ray(self, address: Optional[str]):
        """Initialize Ray runtime with safety checks"""
        if not ray.is_initialized():
            ray_conf = {
                "runtime_env": {"working_dir": os.getcwd()},
                "log_to_driver": False,
                "include_dashboard": False
            }
            if address:
                ray.init(address=address, **ray_conf)
            else:
                ray.init(**ray_conf)

    def _build_algorithm_config(self, config: PolicyConfig) -> AlgorithmConfig:
        """Convert unified config to RLlib-specific format"""
        return (
            AlgorithmConfig()
            .framework("torch")
            .environment(env=None)  # Use custom env through workers
            .rollouts(num_rollout_workers=2)
            .training(**config.hyperparameters)
            .multi_agent(policies={})
            .debugging(log_level="WARN")
        )

    def _load_base_config(self):
        """Load default policies from config file"""
        with open("config/policies/base.yaml") as f:
            base_configs = yaml.safe_load(f)
            for cfg in base_configs["policies"]:
                self.register_policy(**cfg)

    def _sync_policy_weights(self, policy_id: UUID):
        """Distributed weight synchronization via object store"""
        algo = self.get_policy(policy_id)
        weights = algo.get_weights()
        ray.get([
            worker.set_weights.remote(weights)
            for worker in algo.workers.remote_workers()
        ])

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    manager = PolicyManager()
    
    # Register new policy version
    policy_id = manager.register_policy(
        policy_class=rllib.policy.Policy,
        config=PolicyConfig(
            algorithm="PPO",
            hyperparameters={"lr": 0.001, "gamma": 0.99},
            observation_space={"dim": 128},
            action_space={"actions": 10},
            version_constraints={"min_version": "2.1.0"}
        ),
        metadata={"domain": "resource_optimization"}
    )
    
    # Perform training
    print(manager.train_iteration([policy_id]))
    
    # Export for serving
    onnx_model = manager.export_policy(policy_id)
    with open(f"policy_{policy_id}.onnx", "wb") as f:
        f.write(onnx_model)
