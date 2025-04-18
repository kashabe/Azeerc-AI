# dag_parser.py
from __future__ import annotations
import json
import logging
import time
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml
from pydantic import BaseModel, Field, ValidationError
from prometheus_client import Histogram, Counter

logger = logging.getLogger("DAGParser")

#region Prometheus Metrics
DAG_PARSE_TIME = Histogram("dag_parse_seconds", "DAG parsing duration")
DAG_VALIDATION_ERRORS = Counter("dag_validation_errors_total", "Validation failures", ["error_type"])
DAG_CYCLE_DETECTED = Counter("dag_cycles_total", "Cyclic dependencies found")
#endregion

class DAGNode(BaseModel):
    id: str = Field(..., min_length=1)
    task_type: str = Field(..., regex=r"^[a-z0-9_]+$")
    depends_on: List[str] = Field(default_factory=list)
    config: Dict[str, Any] = Field(default_factory=dict)
    timeout: int = Field(300, ge=1)
    retries: int = Field(0, ge=0)

class DAGModel(BaseModel):
    version: str = Field(..., regex=r"^\d+\.\d+\.\d+$")
    nodes: Dict[str, DAGNode] = Field(..., min_items=1)
    workflow_config: Dict[str, Any] = Field(default_factory=dict)

class DAGParser:
    """Enterprise-grade DAG parser with cycle detection and validation"""
    
    def __init__(self, strict_validation: bool = True):
        self.strict_mode = strict_validation
        self._dag_cache: Dict[str, DAGModel] = {}
        self._dependency_graph: Dict[str, Set[str]] = {}
        
    @DAG_PARSE_TIME.time()
    def parse_file(self, file_path: Path) -> Tuple[Dict[str, List[str]], DAGModel]:
        """Parse and validate DAG definition file"""
        try:
            content = file_path.read_text()
            return self.parse_content(content, file_path.suffix.lower())
        except (ValidationError, ValueError) as e:
            logger.error(f"DAG validation failed: {str(e)}")
            DAG_VALIDATION_ERRORS.labels(error_type="schema").inc()
            raise
        except Exception as e:
            logger.error(f"File parsing error: {str(e)}")
            DAG_VALIDATION_ERRORS.labels(error_type="io").inc()
            raise
            
    def parse_content(self, content: str, format_type: str) -> Tuple[Dict[str, List[str]], DAGModel]:
        """Parse raw content with format detection"""
        start_time = time.monotonic()
        
        try:
            if format_type in (".yaml", ".yml"):
                data = yaml.safe_load(content)
            elif format_type == ".json":
                data = json.loads(content)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
        except Exception as e:
            logger.error(f"Content parsing failed: {str(e)}")
            DAG_VALIDATION_ERRORS.labels(error_type="format").inc()
            raise

        try:
            validated_dag = DAGModel(**data)
            self._validate_unique_ids(validated_dag)
            adjacency_map = self._build_dependency_graph(validated_dag)
            
            if self._detect_cycles(validated_dag.nodes.keys(), adjacency_map):
                DAG_CYCLE_DETECTED.inc()
                raise ValueError("Cyclic dependency detected in DAG")
                
            logger.info(f"Parsed DAG in {(time.monotonic()-start_time)*1000:.2f}ms")
            return adjacency_map, validated_dag
            
        except ValidationError as e:
            logger.error(f"DAG validation failed: {str(e)}")
            DAG_VALIDATION_ERRORS.labels(error_type="schema").inc()
            raise
            
    def _validate_unique_ids(self, dag: DAGModel):
        """Ensure node IDs match dictionary keys"""
        for node_id, node in dag.nodes.items():
            if node_id != node.id:
                raise ValueError(f"Node ID mismatch: {node_id} vs {node.id}")
                
    def _build_dependency_graph(self, dag: DAGModel) -> Dict[str, Set[str]]:
        """Generate adjacency list representation"""
        graph = {node_id: set() for node_id in dag.nodes}
        
        for node_id, node in dag.nodes.items():
            for dependency in node.depends_on:
                if dependency not in dag.nodes:
                    msg = f"Undefined dependency: {dependency} in {node_id}"
                    DAG_VALIDATION_ERRORS.labels(error_type="dependency").inc()
                    if self.strict_mode:
                        raise ValueError(msg)
                    logger.warning(msg)
                    continue
                graph[dependency].add(node_id)
                
        return graph
    
    def _detect_cycles(self, nodes: List[str], graph: Dict[str, Set[str]]) -> bool:
        """Kahn's algorithm for topological sorting"""
        in_degree = {node: 0 for node in nodes}
        for node in graph:
            for neighbor in graph[node]:
                in_degree[neighbor] += 1
                
        queue = deque([node for node in in_degree if in_degree[node] == 0])
        visited = 0
        
        while queue:
            current = queue.popleft()
            visited += 1
            
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
                    
        return visited != len(nodes)

class DAGValidator:
    """Extended validation rules for production environments"""
    
    @staticmethod
    def validate_resources(dag: DAGModel, max_memory: int = 1024):
        """Check resource requirements against limits"""
        for node_id, node in dag.nodes.items():
            if node.config.get("memory", 0) > max_memory:
                raise ValueError(f"Memory limit exceeded for {node_id}")
                
    @staticmethod
    def validate_timeouts(dag: DAGModel):
        """Ensure cumulative timeout doesn't exceed thresholds"""
        total_timeout = sum(node.timeout for node in dag.nodes.values())
        if total_timeout > 86400:  # 24 hours
            raise ValueError("Total workflow timeout exceeds 24 hours")

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    sample_yaml = """
    version: 1.2.0
    nodes:
      preprocess:
        task_type: data_processing
        depends_on: [validate]
        timeout: 600
        config:
          input_path: /data/raw
      validate:
        task_type: quality_check
        retries: 3
    workflow_config:
      max_retries: 5
    """
    
    parser = DAGParser(strict_validation=True)
    try:
        adjacency_map, dag_model = parser.parse_content(sample_yaml, ".yaml")
        print(f"Valid DAG with {len(dag_model.nodes)} nodes")
    except Exception as e:
        print(f"DAG validation failed: {str(e)}")
