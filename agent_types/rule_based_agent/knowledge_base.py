# knowledge_base.py
import hashlib
import json
import logging
import os
import re
import semver
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set
from uuid import uuid4

# Constants
RULE_STORAGE_ROOT = Path("/var/azeerc/rules")
SCHEMA_DIR = Path(__file__).parent / "schemas"
MAX_RULE_SIZE = 1024 * 1024  # 1MB per rule
LOG = logging.getLogger("KnowledgeBase")

@dataclass(frozen=True)
class RuleID:
    namespace: str
    name: str
    version: str

@dataclass
class RuleDependency:
    rule_id: RuleID
    version_range: str

@dataclass
class RuleMetadata:
    author: str
    checksum: str
    dependencies: List[RuleDependency]
    signature: Optional[str] = None

class KnowledgeBase:
    def __init__(self, enable_validation: bool = True):
        self._rules: Dict[RuleID, str] = {}
        self._metadata: Dict[RuleID, RuleMetadata] = {}
        self._enable_validation = enable_validation
        self._init_storage()

    def _init_storage(self) -> None:
        """Initialize filesystem structure"""
        RULE_STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
        SCHEMA_DIR.mkdir(exist_ok=True)

    def _validate_semver(self, version: str) -> None:
        """Enforce semantic versioning"""
        if not semver.VersionInfo.isvalid(version):
            raise ValueError(f"Invalid semver: {version}")

    def _validate_rule_content(self, content: str) -> None:
        """Security and syntax validation"""
        if len(content) > MAX_RULE_SIZE:
            raise SecurityError("Rule size exceeds limit")

        if re.search(r"(?i)(exec|system|eval)\(", content):
            raise SecurityError("Forbidden operations detected")

        # Validate against JSON Schema if available
        schema_path = SCHEMA_DIR / "rule_schema.json"
        if schema_path.exists():
            import jsonschema
            jsonschema.validate(json.loads(content), json.loads(schema_path.read_text()))

    def _resolve_dependencies(self, dependencies: List[RuleDependency]) -> None:
        """Ensure all dependencies are satisfied"""
        for dep in dependencies:
            if dep.rule_id not in self._rules:
                raise DependencyError(f"Missing dependency: {dep.rule_id}")
            if not semver.match(self._rules[dep.rule_id].version, dep.version_range):
                raise DependencyError(f"Version mismatch for {dep.rule_id}")

    def add_rule(self, rule_id: RuleID, content: str, metadata: RuleMetadata) -> None:
        """Register a new rule with versioning"""
        self._validate_semver(rule_id.version)
        
        if self._enable_validation:
            self._validate_rule_content(content)
            self._resolve_dependencies(metadata.dependencies)

        rule_path = self._get_storage_path(rule_id)
        rule_path.parent.mkdir(exist_ok=True)
        
        # Prevent overwrites
        if rule_path.exists():
            raise VersionConflictError(f"Rule {rule_id} already exists")

        # Write content and metadata
        rule_path.write_text(content)
        self._rules[rule_id] = content
        self._metadata[rule_id] = metadata
        LOG.info(f"Added rule: {rule_id}")

    def get_rule(self, rule_id: RuleID) -> str:
        """Retrieve rule content with version check"""
        if rule_id not in self._rules:
            raise LookupError(f"Rule {rule_id} not found")
        return self._rules[rule_id]

    def list_versions(self, namespace: str, name: str) -> List[str]:
        """Get all versions for a rule"""
        return sorted(
            [rid.version for rid in self._rules.keys() 
             if rid.namespace == namespace and rid.name == name],
            key=semver.VersionInfo.parse
        )

    def rollback_rule(self, rule_id: RuleID) -> None:
        """Revert to previous version"""
        versions = self.list_versions(rule_id.namespace, rule_id.name)
        current_index = versions.index(rule_id.version)
        
        if current_index == 0:
            raise VersionError("No previous version available")
        
        target_version = versions[current_index - 1]
        target_id = RuleID(rule_id.namespace, rule_id.name, target_version)
        self._set_active_version(rule_id.namespace, rule_id.name, target_id)

    def _get_storage_path(self, rule_id: RuleID) -> Path:
        """Generate filesystem path for rule storage"""
        return RULE_STORAGE_ROOT / f"{rule_id.namespace}/{rule_id.name}_{rule_id.version}.rule"

    def _set_active_version(self, namespace: str, name: str, target_id: RuleID) -> None:
        """Update symlink to target version"""
        active_path = RULE_STORAGE_ROOT / f"{namespace}/{name}_active.rule"
        if active_path.exists():
            active_path.unlink()
        active_path.symlink_to(self._get_storage_path(target_id))

    def verify_integrity(self) -> Set[RuleID]:
        """Check all rules against stored checksums"""
        corrupted = set()
        for rid, meta in self._metadata.items():
            current_hash = hashlib.sha256(self._rules[rid].encode()).hexdigest()
            if current_hash != meta.checksum:
                corrupted.add(rid)
        return corrupted

class SecurityError(Exception):
    """Rule validation failure"""

class VersionConflictError(Exception):
    """Duplicate version registration"""

class DependencyError(Exception):
    """Unsatisfied rule dependencies"""

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    kb = KnowledgeBase()
    sample_id = RuleID("workflow", "approval", "1.3.0")
    sample_meta = RuleMetadata(
        author="security-team@azeerc.ai",
        checksum="a1b2c3...",
        dependencies=[
            RuleDependency(
                RuleID("compliance", "gdpr-check", "2.1.x"),
                ">=2.1.0 <3.0.0"
            )
        ]
    )
    
    try:
        kb.add_rule(sample_id, '{"condition": "priority > 8"}', sample_meta)
        print(kb.get_rule(sample_id))
    except SecurityError as e:
        print(f"Validation failed: {e}")
