# skill_registry.py
from __future__ import annotations
import json
import logging
import re
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, TypeAlias

# Type aliases
SemVer: TypeAlias = str
SkillID: TypeAlias = str

class SkillState(Enum):
    REGISTERED = "registered"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"

class RegistryError(Exception):
    """Base exception for registry failures"""

class VersionConflictError(RegistryError):
    """SemVer compatibility violation"""

class CircularDependencyError(RegistryError):
    """Dependency graph contains cycles"""

@dataclass(frozen=True)
class SkillMetadata:
    name: str
    version: SemVer
    dependencies: Dict[SkillID, SemVer] = field(default_factory=dict)
    authors: List[str] = field(default_factory=list)
    description: str = ""
    state: SkillState = SkillState.REGISTERED
    checksum: Optional[str] = None

    def __post_init__(self):
        if not re.match(r"^\d+\.\d+\.\d+(-[a-zA-Z0-9\.]+)?(\+[a-zA-Z0-9\.]+)?$", self.version):
            raise ValueError(f"Invalid SemVer: {self.version}")
        
        if not re.match(r"^[a-z0-9_\-]+$", self.name):
            raise ValueError(f"Invalid skill name: {self.name}")

class SkillRegistry:
    def __init__(self, persist_path: Optional[Path] = None):
        self.logger = logging.getLogger("SkillRegistry")
        self._lock = threading.RLock()
        self._skills: Dict[SkillID, SkillMetadata] = {}
        self._dependency_graph: Dict[SkillID, Set[SkillID]] = {}
        self.persist_path = persist_path
        
        if self.persist_path and self.persist_path.exists():
            self._load_from_disk()

    def _validate_dependency_graph(self, skill_id: SkillID):
        """Detect circular dependencies using DFS"""
        visited = set()
        stack = [skill_id]
        while stack:
            node = stack.pop()
            if node in visited:
                raise CircularDependencyError(f"Circular dependency detected at {node}")
            visited.add(node)
            stack.extend(self._dependency_graph.get(node, set()))

    def _semver_satisfies(self, required: SemVer, available: SemVer) -> bool:
        """SemVer constraint checker (simplified)"""
        req_main = required.split(".")[:3]
        avail_main = available.split(".")[:3]
        return req_main <= avail_main

    def _resolve_dependencies(self, dependencies: Dict[SkillID, SemVer]) -> List[SkillMetadata]:
        resolved = []
        for dep_id, req_version in dependencies.items():
            if dep_id not in self._skills:
                raise RegistryError(f"Missing dependency: {dep_id}")
                
            if not self._semver_satisfies(req_version, self._skills[dep_id].version):
                raise VersionConflictError(
                    f"{dep_id} requires {req_version} but found {self._skills[dep_id].version}"
                )
                
            resolved.append(self._skills[dep_id])
            resolved += self._resolve_dependencies(self._skills[dep_id].dependencies)
            
        return resolved

    def _load_from_disk(self):
        """Load registry state from JSON"""
        try:
            with self._lock:
                with open(self.persist_path, "r") as f:
                    data = json.load(f)
                    self._skills = {
                        k: SkillMetadata(**v) 
                        for k, v in data["skills"].items()
                    }
                    self._dependency_graph = data["dependency_graph"]
        except Exception as e:
            self.logger.error(f"Failed to load registry: {str(e)}")
            raise

    def _persist_to_disk(self):
        """Save registry state to JSON"""
        if not self.persist_path:
            return
            
        try:
            with self._lock:
                data = {
                    "skills": {k: vars(v) for k, v in self._skills.items()},
                    "dependency_graph": {
                        k: list(v) 
                        for k, v in self._dependency_graph.items()
                    }
                }
                with open(self.persist_path, "w") as f:
                    json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Failed to persist registry: {str(e)}")
            raise

    def register(self, metadata: SkillMetadata) -> List[SkillMetadata]:
        """Register new skill with dependency validation"""
        with self._lock:
            skill_id = f"{metadata.name}@{metadata.version}"
            
            if skill_id in self._skills:
                raise RegistryError(f"Skill already exists: {skill_id}")
                
            # Validate dependency graph
            self._dependency_graph[skill_id] = set(metadata.dependencies.keys())
            self._validate_dependency_graph(skill_id)
            
            # Resolve and validate dependencies
            resolved_deps = self._resolve_dependencies(metadata.dependencies)
            
            # Add to registry
            self._skills[skill_id] = metadata
            self._persist_to_disk()
            
            return resolved_deps

    def deregister(self, skill_id: SkillID) -> None:
        """Remove skill from registry"""
        with self._lock:
            if skill_id not in self._skills:
                raise RegistryError(f"Skill not found: {skill_id}")
                
            # Check if other skills depend on this
            for _, deps in self._dependency_graph.items():
                if skill_id in deps:
                    raise RegistryError(
                        f"Cannot remove {skill_id} - required by other skills"
                    )
                    
            del self._skills[skill_id]
            del self._dependency_graph[skill_id]
            self._persist_to_disk()

    def get_skill(self, name: str, version_constraint: SemVer = "*") -> SkillMetadata:
        """Find best matching version for a skill"""
        with self._lock:
            candidates = [
                meta for meta in self._skills.values() 
                if meta.name == name 
                and self._semver_satisfies(version_constraint, meta.version)
            ]
            
            if not candidates:
                raise RegistryError(f"No matching version for {name}:{version_constraint}")
                
            # Return highest compatible version
            return sorted(
                candidates, 
                key=lambda x: list(map(int, x.version.split(".")[:3])),
                reverse=True
            )[0]

# Example Usage
if __name__ == "__main__":
    registry = SkillRegistry(Path("skill_registry.json"))
    
    # Register base skill
    base_skill = SkillMetadata(
        name="image_processing",
        version="1.2.0",
        description="Basic image manipulation"
    )
    registry.register(base_skill)
    
    # Register dependent skill
    advanced_skill = SkillMetadata(
        name="object_detection",
        version="2.1.3",
        dependencies={"image_processing@1.2.0": ">=1.0.0"},
        authors=["AI Team"],
    )
    dependencies = registry.register(advanced_skill)
    print(f"Resolved dependencies: {[d.name for d in dependencies]}")
    
    # Query skills
    best_image_processor = registry.get_skill("image_processing", ">=1.1.0")
    print(f"Best match: {best_image_processor.version}")
