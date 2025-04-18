# rule_engine.py
from __future__ import annotations
import logging
import os
import re
import hashlib
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
import pyke
from pyke import knowledge_engine, rule_base, goal

# Constants
RULE_DIR = Path(__file__).parent / "rule_repository"
SANDBOX_DIR = Path(tempfile.gettempdir()) / "rule_sandboxes"
DEFAULT_VERSION = "1.0.0"

@dataclass(frozen=True)
class RuleIdentifier:
    name: str
    version: str
    namespace: str = "global"

class RuleEngine:
    def __init__(self, enable_sandboxing: bool = True):
        self.engine = knowledge_engine.engine(__file__)
        self.rule_bases: Dict[str, rule_base.RuleBase] = {}
        self.active_rules: Dict[RuleIdentifier, str] = {}
        self.sandbox_enabled = enable_sandboxing
        self.logger = logging.getLogger("RuleEngine")
        self._init_filesystem()

    def _init_filesystem(self) -> None:
        """Initialize required directories"""
        RULE_DIR.mkdir(exist_ok=True, parents=True)
        SANDBOX_DIR.mkdir(exist_ok=True)

    def _validate_rule_syntax(self, rule_content: str) -> None:
        """Security validation for rule syntax"""
        if re.search(r"(?i)exec\(|os\.system", rule_content):
            raise SecurityError("Forbidden system calls in rule")
        # Add more injection checks as needed

    def load_rule(self, rule_id: RuleIdentifier, rule_content: str) -> None:
        """Load a new rule with versioning"""
        self._validate_rule_syntax(rule_content)
        
        # Store original rule
        rule_path = RULE_DIR / f"{rule_id.namespace}/{rule_id.name}_{rule_id.version}.krb"
        rule_path.parent.mkdir(exist_ok=True)
        rule_path.write_text(rule_content)
        self.active_rules[rule_id] = str(rule_path)

        # Compile into Pyke format
        self.engine.add_rules(rule_content)
        self.engine.activate(f"{rule_id.namespace}_rb")

    @contextmanager
    def sandboxed_session(self) -> Any:
        """Execute rules in isolated environment"""
        if not self.sandbox_enabled:
            yield self.engine
            return

        # Create temporary rule base copy
        sandbox_id = hashlib.sha256(os.urandom(16)).hexdigest()
        sandbox_path = SANDBOX_DIR / sandbox_id
        sandbox_path.mkdir()

        try:
            # Copy relevant rules
            for rule_id, path in self.active_rules.items():
                target = sandbox_path / f"{rule_id.name}.krb"
                target.write_text(Path(path).read_text())

            # Create isolated engine
            sandbox_engine = knowledge_engine.engine(sandbox_path)
            sandbox_engine.reset()
            for rb in self.rule_bases.values():
                sandbox_engine.add_rb(rb.clone())

            sandbox_engine.activate_all()
            yield sandbox_engine
        finally:
            # Cleanup sandbox
            for f in sandbox_path.glob("*"):
                f.unlink()
            sandbox_path.rmdir()

    def infer_forward(self, facts: Dict[str, Any], goal_name: str) -> List[Dict[str, Any]]:
        """Perform forward-chaining inference"""
        with self.sandboxed_session() as engine:
            engine.reset()
            engine.add_facts(facts)
            engine.run()

            results = []
            with engine.prove_goal(goal.compile(goal_name)) as gen:
                for vars, _plan in gen:
                    results.append(vars)
            return results

    def infer_backward(self, hypothesis: Dict[str, Any]) -> bool:
        """Perform backward-chaining validation"""
        with self.sandboxed_session() as engine:
            engine.reset()
            engine.add_facts(hypothesis)
            return engine.prove()

    def rollback_version(self, rule_id: RuleIdentifier) -> None:
        """Revert to previous rule version"""
        versions = sorted(
            [f.stem.split("_")[-1] for f in RULE_DIR.glob(f"{rule_id.name}_*.krb")],
            key=lambda v: list(map(int, v.split('.')))
        )
        if len(versions) < 2:
            raise ValueError("No previous version available")
        
        target_version = versions[-2]
        self.load_rule(
            RuleIdentifier(rule_id.name, target_version, rule_id.namespace),
            (RULE_DIR / f"{rule_id.name}_{target_version}.krb").read_text()
        )

    def export_rule_package(self, namespace: str) -> bytes:
        """Package rules for deployment"""
        from zipfile import ZipFile, ZIP_DEFLATED
        buffer = io.BytesIO()
        with ZipFile(buffer, 'w', ZIP_DEFLATED) as zipf:
            for rule_id in self.active_rules:
                if rule_id.namespace == namespace:
                    zipf.write(
                        self.active_rules[rule_id],
                        arcname=f"{rule_id.name}_{rule_id.version}.krb"
                    )
        buffer.seek(0)
        return buffer.read()

class SecurityError(Exception):
    """Custom exception for rule validation failures"""

# Example Rule Content
SAMPLE_RULE = """
% This is a sample Pyke rule
def is_high_priority(agent, $priority):
    if priority > 7
    then
        assert(requires_approval(agent))

inference_rule:
    if fact:agent_status($agent, $status)
    and check_threshold($status)
    then
        workflow:escalate($agent)
"""

# Usage Example
if __name__ == "__main__":
    from pprint import pprint

    engine = RuleEngine()
    sample_id = RuleIdentifier("priority_check", "1.0.0")
    
    try:
        engine.load_rule(sample_id, SAMPLE_RULE)
        
        # Forward-chaining example
        facts = {"agent_status": {"agent1": "overloaded"}}
        results = engine.infer_forward(facts, "workflow.escalate")
        pprint(results)
        
        # Backward-chaining example
        hypothesis = {"requires_approval": "agent2"}
        valid = engine.infer_backward(hypothesis)
        print(f"Hypothesis valid: {valid}")
        
    except SecurityError as e:
        print(f"Rule validation failed: {str(e)}")
