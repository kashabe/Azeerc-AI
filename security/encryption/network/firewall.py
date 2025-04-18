# firewall.py
import asyncio
import ipaddress
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from cryptography.fernet import Fernet
from prometheus_client import Counter, Histogram
from pydantic import BaseModel

logger = logging.getLogger("ZeroTrustFirewall")

# Prometheus Metrics
POLICY_VIOLATIONS = Counter("zt_policy_violations", "Violation events", ["rule_id", "severity"])
LATENCY_HISTOGRAM = Histogram("zt_policy_check_latency", "Policy evaluation latency")

class PolicyViolation(Exception):
    def __init__(self, rule_id: str, context: Dict):
        super().__init__(f"Violated policy {rule_id}")
        self.rule_id = rule_id
        self.context = context

class ZeroTrustPolicy(BaseModel):
    id: str
    description: str
    priority: int
    enabled: bool
    conditions: Dict[str, Any]
    action: str  # ALLOW/DENY/ALERT
    exceptions: List[str]

class PolicyEngine:
    def __init__(self):
        self.active_policies: Dict[str, ZeroTrustPolicy] = {}
        self._policy_lock = asyncio.Lock()
        self._compiled_conditions: Dict[str, Callable] = {}

    async def load_policies(self, policy_json: str):
        """Hot-reload policies without downtime"""
        new_policies = [ZeroTrustPolicy(**p) for p in json.loads(policy_json)]
        async with self._policy_lock:
            self.active_policies = {p.id: p for p in new_policies if p.enabled}
            self._compile_conditions()

    def _compile_conditions(self):
        """Precompile policy logic for performance"""
        for pid, policy in self.active_policies.items():
            self._compiled_conditions[pid] = self._create_condition_fn(policy.conditions)

    def _create_condition_fn(self, conditions: Dict) -> Callable[[Dict], bool]:
        """Generate executable policy check logic"""
        # Implement policy DSL parser here (e.g., Rego-like logic)
        return lambda ctx: self._evaluate_condition(conditions, ctx)

    async def evaluate_request(self, context: Dict) -> str:
        """Return final decision after evaluating all policies"""
        start_time = time.monotonic()
        try:
            for pid in sorted(self.active_policies.values(), key=lambda x: x.priority):
                if await self._check_policy(pid, context):
                    return pid.action
            return "DENY"  # Default deny
        finally:
            LATENCY_HISTOGRAM.observe(time.monotonic() - start_time)

    async def _check_policy(self, policy: ZeroTrustPolicy, context: Dict) -> bool:
        """Evaluate single policy against request context"""
        if context['source_id'] in policy.exceptions:
            return False

        if self._compiled_conditions[policy.id](context):
            if policy.action == "DENY":
                POLICY_VIOLATIONS.labels(policy.id, "critical").inc()
                raise PolicyViolation(policy.id, context)
            elif policy.action == "ALERT":
                POLICY_VIOLATIONS.labels(policy.id, "warning").inc()
            return True
        return False

class TrafficAnalyzer:
    def __init__(self):
        self.known_threats: Set[str] = set()
        self._behavior_profiles: Dict[str, Dict] = {}

    async def analyze_patterns(self, flow: Dict):
        """Detect DDoS/PortScanning/Anomalies"""
        # Integrate with ML models here
        if flow['request_count'] > 1000 and flow['unique_endpoints'] > 50:
            self.known_threats.add(flow['source_ip'])

class ZeroTrustFirewall:
    def __init__(self, policy_engine: PolicyEngine, analyzer: TrafficAnalyzer):
        self.engine = policy_engine
        self.analyzer = analyzer
        self.active_blocks: Dict[str, float] = {}
        self._key = Fernet.generate_key()

    async def process_request(self, context: Dict) -> Dict:
        """Main firewall processing pipeline"""
        if await self._check_automated_threats(context):
            return {"action": "BLOCK", "reason": "Known threat"}

        try:
            action = await self.engine.evaluate_request(context)
            return {"action": action, "context": context}
        except PolicyViolation as e:
            await self._enforce_block(e.context['source_ip'])
            return {"action": "BLOCK", "rule_id": e.rule_id}

    async def _check_automated_threats(self, context: Dict) -> bool:
        """Check IP reputation and behavior patterns"""
        ip = context['source_ip']
        if ip in self.active_blocks:
            if time.time() - self.active_blocks[ip] < 3600:
                return True
            del self.active_blocks[ip]
        return False

    async def _enforce_block(self, identifier: str):
        """Dynamic blocking with TTL"""
        self.active_blocks[identifier] = time.time()
        logger.warning(f"Blocking suspicious host: {identifier}")

# Example Policy JSON
SAMPLE_POLICY = '''
[
  {
    "id": "zt-001",
    "description": "Require MFA for admin access",
    "priority": 1,
    "enabled": true,
    "conditions": {
      "resource": "/admin/*",
      "auth_level": "< 2",
      "time": {
        "window": "09:00-17:00",
        "days": ["Mon-Fri"]
      }
    },
    "action": "DENY",
    "exceptions": ["breakglass-01"]
  }
]
'''

# Integration Example
if __name__ == "__main__":
    engine = PolicyEngine()
    analyzer = TrafficAnalyzer()
    firewall = ZeroTrustFirewall(engine, analyzer)

    # Load policies from external source
    asyncio.run(engine.load_policies(SAMPLE_POLICY))

    # Simulate request
    sample_context = {
        "source_ip": "192.168.1.100",
        "source_id": "user-123",
        "resource": "/admin/dashboard",
        "auth_level": 1,
        "timestamp": datetime.utcnow().isoformat()
    }

    decision = asyncio.run(firewall.process_request(sample_context))
    print(f"Firewall decision: {decision}")
