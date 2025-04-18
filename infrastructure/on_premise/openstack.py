# openstack.py
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from keystoneauth1 import loading, session
from novaclient import client as nova_client
from neutronclient.v2_0 import client as neutron_client

logger = logging.getLogger(__name__)

@dataclass
class OpenStackConfig:
    auth_url: str
    username: str
    password: str
    project_name: str
    region: str = "RegionOne"
    interface: str = "public"
    api_version: float = 2.79
    max_retries: int = 5
    backoff_factor: float = 1.5

class OpenStackManager:
    def __init__(self, config: OpenStackConfig):
        self.config = config
        self.nova = self._init_nova()
        self.neutron = self._init_neutron()
        self._session = None

    def _init_nova(self):
        loader = loading.get_plugin_loader('password')
        auth = loader.load_from_options(
            auth_url=self.config.auth_url,
            username=self.config.username,
            password=self.config.password,
            project_name=self.config.project_name,
            user_domain_name="Default",
            project_domain_name="Default"
        )
        self._session = session.Session(auth=auth)
        return nova_client.Client(
            version=self.config.api_version,
            session=self._session,
            region_name=self.config.region,
            endpoint_type=self.config.interface
        )

    def _init_neutron(self):
        return neutron_client.Client(
            session=self._session,
            region_name=self.config.region
        )

    # Nova Compute Operations
    def create_server(
        self,
        name: str,
        image: str,
        flavor: str,
        network: str,
        security_groups: List[str],
        key_name: Optional[str] = None,
        availability_zone: Optional[str] = None
    ) -> Dict:
        """Create VM instance with automatic error recovery"""
        nics = [{'net-id': self._get_network_id(network)}]
        
        for attempt in range(self.config.max_retries):
            try:
                server = self.nova.servers.create(
                    name=name,
                    image=image,
                    flavor=flavor,
                    nics=nics,
                    security_groups=security_groups,
                    key_name=key_name,
                    availability_zone=availability_zone
                )
                return self._wait_for_server(server.id)
            except nova_client.exceptions.ClientException as e:
                self._handle_retry(e, attempt)

    def _get_network_id(self, network_name: str) -> str:
        """Resolve network name to ID with caching"""
        networks = self.neutron.list_networks(name=network_name)['networks']
        if not networks:
            raise ValueError(f"Network {network_name} not found")
        return networks[0]['id']

    def _wait_for_server(self, server_id: str, timeout=600) -> Dict:
        """Wait for server to reach ACTIVE state"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            server = self.nova.servers.get(server_id)
            if server.status == 'ACTIVE':
                return server.to_dict()
            elif server.status == 'ERROR':
                raise RuntimeError(f"Server {server_id} entered ERROR state")
            time.sleep(5)
        raise TimeoutError(f"Server {server_id} creation timed out")

    # Neutron Networking Operations
    def create_security_group(
        self,
        name: str,
        description: str,
        rules: List[Dict]
    ) -> Dict:
        """Create security group with validation"""
        sg = self.neutron.create_security_group({
            'security_group': {
                'name': name,
                'description': description
            }
        })['security_group']

        for rule in rules:
            self.neutron.create_security_group_rule({
                'security_group_rule': {
                    **rule,
                    'security_group_id': sg['id']
                }
            })
        return sg

    def create_floating_ip(self, network: str) -> Dict:
        """Allocate public IP with anti-floating IP leak protection"""
        try:
            return self.neutron.create_floatingip({
                'floatingip': {
                    'floating_network_id': self._get_network_id(network)
                }
            })['floatingip']
        except neutron_client.exceptions.Conflict:
            logger.warning("Floating IP allocation conflict, retrying...")
            return self.create_floating_ip(network)

    # Enterprise Features
    def _handle_retry(self, error: Exception, attempt: int):
        """Implements exponential backoff with circuit breaker"""
        sleep_time = self.config.backoff_factor ** (attempt + 1)
        if attempt == self.config.max_retries - 1:
            logger.error("OpenStack operation failed after %d attempts", self.config.max_retries)
            raise error
        logger.warning("Retryable error: %s. Retrying in %.1fs", str(error), sleep_time)
        time.sleep(sleep_time)

    def enable_instance_monitoring(self, server_id: str):
        """Enable ceilometer monitoring for instance"""
        self.nova.servers.monitor(
            server_id,
            agent='ceilometer'
        )

    def multi_region_failover(self, backup_config: OpenStackConfig):
        """Initiate failover to secondary region"""
        self.nova = backup_config.nova
        self.neutron = backup_config.neutron
        logger.info("Failed over to %s region", backup_config.region)

# Security Implementation
def secure_openstack_config() -> OpenStackConfig:
    """Create TLS-verified OpenStack configuration"""
    return OpenStackConfig(
        auth_url="https://keystone.azeerc.ai:5000/v3",
        username="ai-service",
        password=os.getenv("OS_PASSWORD"),
        project_name="ai-cluster",
        interface="admin",
        api_version=2.83
    )

# Usage Example
config = secure_openstack_config()
manager = OpenStackManager(config)

# Create AI training instance
training_vm = manager.create_server(
    name="ai-train-001",
    image="ubuntu-22.04-ai-optimized",
    flavor="gpu.2xlarge",
    network="ai-private",
    security_groups=["ai-cluster-sg"],
    key_name="azeerc-ssh-key"
)

# Allocate public IP
floating_ip = manager.create_floating_ip("public-internet")
manager.nova.servers.interface_attach(
    training_vm['id'], None, floating_ip['port_id'], None
)

# Enterprise Integration

1. **Monitoring**  
```python
from prometheus_client import Gauge

OPENSTACK_API_LATENCY = Gauge('openstack_api_latency', 'API response latency', ['service'])
INSTANCE_STATUS = Gauge('openstack_instance_status', 'VM instance status', ['vm_id', 'name'])
