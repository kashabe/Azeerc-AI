# service_registry.py
import etcd3
import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Callable

# Constants
DEFAULT_LEASE_TTL = 30  # Seconds
HEARTBEAT_INTERVAL = 10  # Seconds
RETRY_POLICY = {
    'max_attempts': 3,
    'backoff_factor': 0.5  # Exponential backoff
}

class RegistryError(Exception):
    """Base exception for service registry failures"""

class ServiceRegistry(ABC):
    @abstractmethod
    def register_service(self, service: ServiceInfo) -> None:
        """Register service instance with metadata"""
        pass
    
    @abstractmethod
    def deregister_service(self, service_id: str) -> None:
        """Remove service instance"""
        pass
    
    @abstractmethod
    def discover_services(
        self, 
        service_name: str, 
        filters: Optional[Dict[str, str]] = None
    ) -> List[ServiceInfo]:
        """Discover instances matching criteria"""
        pass
    
    @abstractmethod
    def start_heartbeat(self) -> None:
        """Start background keepalive"""
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """Cleanup resources"""
        pass

@dataclass
class ServiceInfo:
    name: str
    endpoint: str  # "host:port"
    protocol: str = "grpc"
    version: str = "1.0.0"
    metadata: Dict[str, str] = field(default_factory=dict)
    ttl: int = DEFAULT_LEASE_TTL
    service_id: Optional[str] = None  # Auto-generated

class EtcdServiceRegistry(ServiceRegistry):
    def __init__(
        self, 
        etcd_hosts: List[str] = ['localhost'],
        etcd_port: int = 2379,
        ca_cert: Optional[str] = None,
        client_cert: Optional[Tuple[str, str]] = None
    ):
        self.logger = logging.getLogger("ServiceRegistry")
        self._lock = threading.RLock()
        self._etcd = etcd3.client(
            host=etcd_hosts,
            port=etcd_port,
            ca_cert=ca_cert,
            cert=client_cert
        )
        self._leases: Dict[str, etcd3.Lease] = {}
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._running = False

    def _make_key(self, service: ServiceInfo) -> str:
        return f"/azeerc/services/{service.name}/{service.service_id}"

    def _serialize_service(self, service: ServiceInfo) -> bytes:
        return json.dumps({
            'endpoint': service.endpoint,
            'protocol': service.protocol,
            'version': service.version,
            'metadata': service.metadata,
            'ttl': service.ttl
        }).encode('utf-8')

    def _deserialize_service(self, key: bytes, value: bytes) -> ServiceInfo:
        data = json.loads(value.decode('utf-8'))
        service_id = key.decode('utf-8').split('/')[-1]
        return ServiceInfo(
            name='/'.join(key.decode('utf-8').split('/')[3:-1]),
            service_id=service_id,
            endpoint=data['endpoint'],
            protocol=data['protocol'],
            version=data['version'],
            metadata=data['metadata'],
            ttl=data['ttl']
        )

    def register_service(self, service: ServiceInfo) -> None:
        with self._lock:
            if not service.service_id:
                service.service_id = f"{service.endpoint}-{int(time.time())}"
            
            key = self._make_key(service)
            lease = self._etcd.lease(service.ttl)
            self._etcd.put(key, self._serialize_service(service), lease=lease)
            self._leases[service.service_id] = lease
            self.logger.info(f"Registered service {service.service_id}")

    def deregister_service(self, service_id: str) -> None:
        with self._lock:
            if service_id not in self._leases:
                raise RegistryError(f"Service not registered: {service_id}")
            
            lease = self._leases.pop(service_id)
            lease.revoke()
            self.logger.info(f"Deregistered service {service_id}")

    def discover_services(
        self, 
        service_name: str, 
        filters: Optional[Dict[str, str]] = None
    ) -> List[ServiceInfo]:
        prefix = f"/azeerc/services/{service_name}/"
        try:
            response = self._etcd.get_prefix(prefix)
            services = [
                self._deserialize_service(k, v) 
                for k, v in response
            ]
            
            if filters:
                services = [
                    s for s in services
                    if all(
                        s.metadata.get(k) == v 
                        for k, v in filters.items()
                    )
                ]
                
            return services
        except etcd3.exceptions.Etcd3Exception as e:
            self.logger.error(f"Discovery failed: {str(e)}")
            raise RegistryError("Service discovery unavailable") from e

    def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                with self._lock:
                    for service_id, lease in self._leases.items():
                        try:
                            lease.refresh()
                            self.logger.debug(f"Refreshed lease for {service_id}")
                        except etcd3.exceptions.Etcd3Exception as e:
                            self.logger.error(f"Heartbeat failed for {service_id}: {str(e)}")
                            del self._leases[service_id]
                time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                self.logger.critical(f"Heartbeat loop crashed: {str(e)}")
                break

    def start_heartbeat(self) -> None:
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
            
        self._running = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True
        )
        self._heartbeat_thread.start()

    def stop(self) -> None:
        self._running = False
        with self._lock:
            for lease in self._leases.values():
                try:
                    lease.revoke()
                except Exception as e:
                    self.logger.error(f"Lease revocation failed: {str(e)}")
            self._leases.clear()

        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5)

class LoadBalancer:
    def __init__(self, registry: ServiceRegistry):
        self.registry = registry
        self._strategy: Callable[[List[ServiceInfo]], ServiceInfo] = self.round_robin
        self._cursor = 0

    def set_strategy(self, strategy: Callable[[List[ServiceInfo]], ServiceInfo]) -> None:
        self._strategy = strategy

    def round_robin(self, services: List[ServiceInfo]) -> ServiceInfo:
        with threading.Lock():
            self._cursor = (self._cursor + 1) % len(services)
            return services[self._cursor]

    def get_instance(self, service_name: str, **filters) -> ServiceInfo:
        services = self.registry.discover_services(service_name, filters)
        if not services:
            raise RegistryError(f"No instances available for {service_name}")
        return self._strategy(services)

# Example Usage
if __name__ == "__main__":
    import sys
    
    logging.basicConfig(level=logging.INFO)
    
    # Initialize registry
    registry = EtcdServiceRegistry(etcd_hosts=['etcd1:2379', 'etcd2:2379'])
    
    # Register sample service
    svc = ServiceInfo(
        name="image_processor",
        endpoint="10.0.1.5:50051",
        protocol="grpc",
        version="2.1.0",
        metadata={"env": "prod", "region": "us-west"}
    )
    registry.register_service(svc)
    
    # Start heartbeat background thread
    registry.start_heartbeat()
    
    # Service discovery
    try:
        instances = registry.discover_services(
            "image_processor", 
            {"env": "prod"}
        )
        print(f"Found {len(instances)} instances")
        
        # Load balancing
        lb = LoadBalancer(registry)
        selected = lb.get_instance("image_processor")
        print(f"Selected instance: {selected.endpoint}")
        
    finally:
        registry.stop()
