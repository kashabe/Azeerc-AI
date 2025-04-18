# vmware.py
import atexit
import logging
import ssl
import time
from dataclasses import dataclass
from pyVmomi import vim, vmodl
from pyVim.connect import SmartConnect, Disconnect

logger = logging.getLogger(__name__)

@dataclass
class VSphereConfig:
    host: str
    user: str
    password: str
    port: int = 443
    ssl_verify: bool = True
    connection_timeout: int = 30

class VSphereManager:
    def __init__(self, config: VSphereConfig):
        self.config = config
        self.service_instance = None
        self.content = None
        self.stats_interval = 20  # Performance stats collection interval

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self, retries=3, backoff_factor=2):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if not self.config.ssl_verify:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

        for attempt in range(retries):
            try:
                self.service_instance = SmartConnect(
                    host=self.config.host,
                    user=self.config.user,
                    pwd=self.config.password,
                    port=self.config.port,
                    sslContext=context,
                    connectionTimeoutSec=self.config.connection_timeout
                )
                self.content = self.service_instance.RetrieveContent()
                atexit.register(Disconnect, self.service_instance)
                logger.info("Connected to vCenter %s", self.config.host)
                return
            except (vmodl.RuntimeFault, ConnectionError) as e:
                if attempt == retries -1:
                    raise RuntimeError(f"vSphere connection failed: {str(e)}")
                sleep_time = backoff_factor ** attempt
                logger.warning("Connection attempt %d failed. Retrying in %ds", attempt+1, sleep_time)
                time.sleep(sleep_time)

    def disconnect(self):
        if self.service_instance:
            Disconnect(self.service_instance)
            logger.info("Disconnected from vCenter")

    def get_vm_by_name(self, vm_name: str) -> vim.VirtualMachine:
        vm_view = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [vim.VirtualMachine], True
        )
        for vm in vm_view.view:
            if vm.name == vm_name:
                return vm
        raise ValueError(f"VM {vm_name} not found")

    def create_vm(
        self,
        vm_name: str,
        resource_pool: str,
        datastore: str,
        template: vim.VirtualMachine = None
    ) -> vim.VirtualMachine:
        cluster = self._find_entity(vim.ClusterComputeResource, resource_pool)
        datastore = self._find_entity(vim.Datastore, datastore)
        host = cluster.host[0]

        if template:
            return self._clone_vm_template(template, vm_name, cluster.resourcePool, datastore)
        else:
            return self._create_new_vm(vm_name, host, cluster.resourcePool, datastore)

    def _find_entity(self, entity_type, name: str):
        entity_view = self.content.viewManager.CreateContainerView(
            self.content.rootFolder, [entity_type], True
        )
        for item in entity_view.view:
            if item.name == name:
                return item
        raise ValueError(f"{entity_type.__name__} {name} not found")

    def _clone_vm_template(self, template: vim.VirtualMachine, vm_name: str, 
                         resource_pool: vim.ResourcePool, datastore: vim.Datastore):
        relocate_spec = vim.vm.RelocateSpec(
            datastore=datastore,
            pool=resource_pool
        )
        clone_spec = vim.vm.CloneSpec(
            location=relocate_spec,
            powerOn=True,
            template=False
        )
        task = template.Clone(folder=template.parent, name=vm_name, spec=clone_spec)
        return self._wait_for_task(task)

    def _create_new_vm(self, vm_name: str, host: vim.HostSystem, 
                      resource_pool: vim.ResourcePool, datastore: vim.Datastore):
        config = vim.vm.ConfigSpec(
            name=vm_name,
            memoryMB=4096,
            numCPUs=2,
            files=vim.vm.FileInfo(
                vmPathName=f"[{datastore.name}] {vm_name}/{vm_name}.vmx"
            ),
            guestId="ubuntu64Guest"
        )
        task = resource_pool.CreateVM_Task(
            config=config,
            folder=self.content.rootFolder,
            host=host
        )
        return self._wait_for_task(task)

    def _wait_for_task(self, task: vim.Task, timeout=600):
        start_time = time.time()
        while task.info.state not in [vim.TaskInfo.State.success, vim.TaskInfo.State.error]:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Task {task.info.key} timed out")
            time.sleep(2)
        if task.info.state == "error":
            raise RuntimeError(f"Task failed: {task.info.error.localizedMessage}")
        return task.info.result

    # Enterprise Features Implementation

    def enable_drs_automation(self, cluster_name: str, automation_level: str = "fullyAutomated"):
        cluster = self._find_entity(vim.ClusterComputeResource, cluster_name)
        config_spec = vim.cluster.ConfigSpecEx(
            drsConfig=vim.cluster.DrsConfigInfo(
                enabled=True,
                defaultVmBehavior=automation_level,
                vmotionRate=3
            )
        )
        task = cluster.ReconfigureComputeResource_Task(config_spec, True)
        return self._wait_for_task(task)

    def migrate_vm_host(self, vm: vim.VirtualMachine, target_host: str):
        host = self._find_entity(vim.HostSystem, target_host)
        resource_pool = host.parent.resourcePool
        relocate_spec = vim.vm.RelocateSpec(host=host, pool=resource_pool)
        task = vm.Relocate(relocate_spec)
        return self._wait_for_task(task)

    def create_snapshot(self, vm: vim.VirtualMachine, name: str, description: str = "", 
                       memory_dump: bool = False, quiesce: bool = True):
        task = vm.CreateSnapshot_Task(
            name=name,
            description=description,
            memory=memory_dump,
            quiesce=quiesce
        )
        return self._wait_for_task(task)

    def get_perf_metrics(self, vm: vim.VirtualMachine):
        perf_manager = self.content.perfManager
        metric_ids = [
            vim.PerformanceManager.MetricId(
                counterId=6,  # CPU usage
                instance=""
            ),
            vim.PerformanceManager.MetricId(
                counterId=33,  # Memory usage
                instance=""
            )
        ]
        query = vim.PerformanceManager.QuerySpec(
            entity=vm,
            metricId=metric_ids,
            intervalId=20,
            maxSample=10
        )
        return perf_manager.QueryStats(querySpec=[query])

# Security Implementation
def enable_vsphere_ssl_verification():
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.check_hostname = True
    return ssl_context

# Usage Example
config = VSphereConfig(
    host="vcenter.azeerc.ai",
    user="svc-azeerc",
    password=os.getenv("VCENTER_PASSWORD"),
    ssl_verify=True
)

with VSphereManager(config) as mgr:
    vm = mgr.get_vm_by_name("AI-Training-Node01")
    mgr.migrate_vm_host(vm, "esxi-node03.azeerc.ai")
    snapshot = mgr.create_snapshot(vm, "Pre-Upgrade")
    mgr.enable_drs_automation("AI-Cluster")

# Enterprise Integration

1. **Monitoring**  
```python
from prometheus_client import Gauge

VCENTER_CONNECTION_STATUS = Gauge('vcenter_connection_status', 'vCenter connectivity status')
VM_POWER_STATE = Gauge('vm_power_state', 'Virtual machine power state', ['vm_name'])
