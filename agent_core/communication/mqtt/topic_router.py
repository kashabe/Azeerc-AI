# topic_router.py
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Callable, Dict, List, Optional, Pattern, Tuple

from prometheus_client import Counter, Histogram

# Metrics
ROUTING_REQUESTS = Counter('azeerc_router_requests_total', 'Total routing requests', ['topic'])
ROUTING_LATENCY = Histogram('azeerc_router_latency_seconds', 'Routing processing time')
ROUTING_ERRORS = Counter('azeerc_router_errors_total', 'Routing errors', ['error_type'])

@dataclass(frozen=True)
class RoutingRule:
    topic_pattern: str
    handler: Callable[[str, Dict[str, Any]], None]
    priority: int = 0
    fallback_handlers: List[Callable[[str, Dict[str, Any]], None]] = None
    qos: int = 2

class TrieNode:
    __slots__ = ('handlers', 'children', 'wildcards')
    
    def __init__(self):
        self.handlers: List[RoutingRule] = []
        self.children: Dict[str, TrieNode] = {}
        self.wildcards: Dict[Pattern, Tuple[TrieNode, bool]] = {}  # (node, is_multi_level)

class TopicRouter:
    def __init__(self):
        self._root = TrieNode()
        self._topic_validator = re.compile(r"^[a-zA-Z0-9\-_\/]+$")
        self._cache = defaultdict(list)
        self._lock = threading.RLock()
        
        # Precompile wildcard patterns
        self._single_wildcard = re.compile(r"[^\/]+")
        self._multi_wildcard = re.compile(r".*")
    
    @ROUTING_LATENCY.time()
    def add_route(self, rule: RoutingRule) -> None:
        """Register routing rule with topic pattern"""
        if not self._validate_topic(rule.topic_pattern):
            raise ValueError(f"Invalid topic pattern: {rule.topic_pattern}")
            
        with self._lock:
            self._cache.clear()
            current_node = self._root
            segments = rule.topic_pattern.split('/')
            
            for i, segment in enumerate(segments):
                is_last = i == len(segments) - 1
                
                # Handle wildcards
                if segment == "+":
                    pattern = self._single_wildcard
                    is_multi = False
                elif segment == "#":
                    pattern = self._multi_wildcard
                    is_multi = True
                else:
                    pattern = None
                
                if pattern:
                    if pattern not in current_node.wildcards:
                        new_node = TrieNode()
                        current_node.wildcards[pattern] = (new_node, is_multi)
                    current_node, _ = current_node.wildcards[pattern]
                else:
                    if segment not in current_node.children:
                        current_node.children[segment] = TrieNode()
                    current_node = current_node.children[segment]
                
                if is_last:
                    current_node.handlers.append(rule)
                    current_node.handlers.sort(key=lambda x: -x.priority)

    def route_message(self, topic: str, payload: Dict[str, Any]) -> None:
        """Route incoming message to matching handlers"""
        if not self._validate_topic(topic):
            ROUTING_ERRORS.labels(error_type='invalid_topic').inc()
            logging.error(f"Rejected invalid topic: {topic}")
            return
            
        with self._lock:
            if topic in self._cache:
                for rule in self._cache[topic]:
                    self._execute_handler(rule, topic, payload)
                return
                
            current_nodes = [self._root]
            segments = topic.split('/')
            matched_rules = []
            
            for segment in segments:
                next_nodes = []
                
                for node in current_nodes:
                    # Exact match
                    if segment in node.children:
                        next_nodes.append(node.children[segment])
                    
                    # Wildcard matches
                    for pattern, (wild_node, is_multi) in node.wildcards.items():
                        if pattern.fullmatch(segment):
                            next_nodes.append(wild_node)
                            if is_multi:
                                next_nodes.extend(self._collect_multi_level(wild_node))
                
                current_nodes = next_nodes
            
            # Collect all rules from final nodes
            for node in current_nodes:
                matched_rules.extend(node.handlers)
            
            # Sort by priority and cache
            matched_rules.sort(key=lambda x: -x.priority)
            self._cache[topic] = matched_rules
            
            for rule in matched_rules:
                self._execute_handler(rule, topic, payload)
    
    def _execute_handler(self, rule: RoutingRule, topic: str, payload: Dict[str, Any]) -> None:
        try:
            ROUTING_REQUESTS.labels(topic=topic).inc()
            rule.handler(topic, payload)
        except Exception as e:
            ROUTING_ERRORS.labels(error_type='handler_failure').inc()
            logging.error(f"Handler failed for {topic}: {str(e)}")
            if rule.fallback_handlers:
                for fallback in rule.fallback_handlers:
                    try:
                        fallback(topic, payload)
                    except Exception as fe:
                        logging.error(f"Fallback handler failed: {str(fe)}")
    
    def _collect_multi_level(self, node: TrieNode) -> List[TrieNode]:
        """Collect all nodes under multi-level wildcard"""
        nodes = []
        stack = [node]
        while stack:
            current = stack.pop()
            nodes.append(current)
            stack.extend(current.children.values())
            for _, (wn, _) in current.wildcards.items():
                stack.append(wn)
        return nodes
    
    @lru_cache(maxsize=1024)
    def _validate_topic(self, topic: str) -> bool:
        """Validate topic format with caching"""
        return bool(self._topic_validator.match(topic))

# Example Usage
if __name__ == "__main__":
    router = TopicRouter()
    
    # Define handlers
    def system_handler(topic: str, payload: Dict[str, Any]):
        print(f"System: {topic} -> {payload}")
    
    def analytics_handler(topic: str, payload: Dict[str, Any]):
        print(f"Analytics: {topic} -> {payload}")
    
    # Add routing rules
    router.add_route(RoutingRule(
        topic_pattern="azeerc/system/#",
        handler=system_handler,
        priority=100,
        qos=2
    ))
    
    router.add_route(RoutingRule(
        topic_pattern="azeerc/analytics/+",
        handler=analytics_handler,
        priority=50,
        qos=1
    ))
    
    # Test routing
    router.route_message("azeerc/system/status", {"status": "OK"})
    router.route_message("azeerc/analytics/metrics", {"cpu": 0.75})
    router.route_message("azeerc/system/alerts/critical", {"alert": "high_load"})
