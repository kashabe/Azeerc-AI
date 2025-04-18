# k3s_cluster.py
import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

logger = logging.getLogger(__name__)

@dataclass
class K3sConfig:
    cluster_name: str
    server_nodes: List[str]
    worker_nodes: List[str]
    k3s_version: str = "v1.27.4+k3s1"
    datastore_endpoint: str = "postgres://k3s:password@db.azeerc.ai:5432/k3s"
    tls_san: List[str] = field(default_factory=lambda: ["api.azeerc.ai"])
    enable_ha: bool = True
    cloud_provider: Optional[str] = "aws"
    node_os: str = "containerd://1.7.3"
    audit_log_path: str = "/var/log/k3s/audit.log"

class K3sManager:
    def __init__(self, config: K3sConfig):
        self.config = config
        self.ca_cert, self.ca_key = self._generate_ca()
        self.kubeconfig = self._generate_kubeconfig()

    def _generate_ca(self) -> tuple[bytes, bytes]:
        """Generate CA certificate for cluster TLS"""
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096,
            backend=default_backend()
        )
        cert = self._self_signed_cert(key, "k3s-ca")
        return (
            cert.public_bytes(serialization.Encoding.PEM),
            key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            )
        )

    def create_cluster(self):
        """Orchestrate HA cluster deployment"""
        try:
            self._install_servers()
            self._join_workers()
            self._configure_autoscaler()
            self._enable_monitoring()
            logger.info(f"Cluster {self.config.cluster_name} deployed successfully")
        except subprocess.CalledProcessError as e:
            logger.error(f"Cluster deployment failed: {e.stderr}")
            raise

    def _install_servers(self):
        """Bootstrap control plane nodes"""
        for idx, node in enumerate(self.config.server_nodes):
            cmd = [
                "curl -sfL https://get.k3s.io |",
                f"INSTALL_K3S_VERSION={self.config.k3s_version}",
                f"K3S_TOKEN=secret",
                "sh -s - server",
                "--cluster-init" if idx == 0 else f"--server https://{self.config.server_nodes[0]}:6443",
                f"--datastore-endpoint={self.config.datastore_endpoint}",
                f"--tls-san={','.join(self.config.tls_san)}",
                "--disable=traefik",
                "--kubelet-arg=eviction-hard=memory.available<500Mi",
                f"--kube-apiserver-arg=audit-log-path={self.config.audit_log_path}"
            ]
            self._ssh_exec(node, " ".join(cmd))

    def _join_workers(self):
        """Join worker nodes to cluster"""
        join_cmd = f"K3S_URL=https://{self.config.server_nodes[0]}:6443 K3S_TOKEN=secret"
        for node in self.config.worker_nodes:
            self._ssh_exec(node, f"curl -sfL https://get.k3s.io | {join_cmd} sh -")

    def _configure_autoscaler(self):
        """Set up cluster autoscaler with cloud integration"""
        config = f"""
        apiVersion: clusterautoscaler.k8s.io/v1beta1
        kind: NodeGroup
        metadata:
          name: {self.config.cluster_name}-workers
        spec:
          cloudProvider: {self.config.cloud_provider}
          nodeLabels:
            node.kubernetes.io/instance-type: optimized
          minSize: 3
          maxSize: 10
        """
        self._apply_manifest(config)

    def _enable_monitoring(self):
        """Deploy Prometheus/Grafana stack"""
        self._apply_manifest("""
        apiVersion: monitoring.coreos.com/v1
        kind: Prometheus
        metadata:
          name: k3s-monitoring
        spec:
          serviceAccountName: prometheus
          resources:
            requests:
              memory: 400Mi
          ruleSelector:
            matchLabels:
              role: alert-rules
          alerting:
            alertmanagers:
            - namespace: monitoring
              name: alertmanager-main
              port: web
        """)

    def _apply_manifest(self, yaml_content: str):
        """Apply Kubernetes manifests safely"""
        with NamedTemporaryFile(mode="w") as f:
            f.write(yaml_content)
            f.flush()
            subprocess.run(
                ["kubectl", "apply", "-f", f.name],
                check=True,
                capture_output=True
            )

    def _ssh_exec(self, host: str, command: str):
        """Execute commands over SSH with retry logic"""
        for attempt in range(3):
            try:
                subprocess.run(
                    ["ssh", f"admin@{host}", command],
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=300
                )
                return
            except subprocess.TimeoutExpired:
                logger.warning(f"SSH timeout on {host}, attempt {attempt+1}")
                time.sleep(5 ** (attempt + 1))

    # Security Features
    def rotate_certs(self):
        """Rotate cluster certificates automatically"""
        subprocess.run(
            ["k3s", "certificate", "rotate"],
            check=True,
            capture_output=True
        )

    def enable_opa(self):
        """Deploy Open Policy Agent for RBAC"""
        self._apply_manifest("""
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: opa
        spec:
          replicas: 3
          selector:
            matchLabels:
              app: opa
          template:
            metadata:
              labels:
                app: opa
            spec:
              containers:
              - name: opa
                image: openpolicyagent/opa:0.44.0
                args: ["run", "--server"]
        """)

    # Disaster Recovery
    def create_snapshot(self):
        """Create etcd snapshot for disaster recovery"""
        subprocess.run(
            ["k3s", "etcd-snapshot", "save"],
            check=True,
            capture_output=True
        )

# Production Deployment Example
config = K3sConfig(
    cluster_name="ai-inference",
    server_nodes=["10.0.1.10", "10.0.1.11", "10.0.1.12"],
    worker_nodes=["10.0.2.10", "10.0.2.11"],
    tls_san=["ai.azeerc.io"],
    cloud_provider="aws"
)

manager = K3sManager(config)
manager.create_cluster()
manager.enable_opa()

# Enterprise Integration

1. **Monitoring Stack**  
```python
from prometheus_client import start_http_server, Gauge

CLUSTER_NODES = Gauge('k3s_nodes_total', 'Total worker nodes')
CLUSTER_PODS = Gauge('k3s_pods_running', 'Running pods count')

def update_metrics():
    nodes = json.loads(subprocess.run(
        ["kubectl", "get", "nodes", "-o", "json"],
        capture_output=True
    ).stdout)
    CLUSTER_NODES.set(len(nodes['items']))
