# ec2_autoscaling.py
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

@dataclass
class SpotInstanceConfig:
    launch_template: str
    instance_types: List[str]
    availability_zones: List[str]
    max_price: str = "on-demand-price"
    allocation_strategy: str = "capacity-optimized"
    fallback_to_ondemand: bool = True
    termination_buffer: int = 120  # Seconds before spot termination
    max_attempts: int = 3

class EC2SpotManager:
    def __init__(self, config: SpotInstanceConfig):
        self.config = config
        self.ec2 = boto3.client(
            'ec2',
            config=Config(
                retries={
                    'max_attempts': config.max_attempts,
                    'mode': 'adaptive'
                }
            )
        )
        self.active_instances = {}
        self.termination_handled = set()

    def _get_price_history(self) -> List[float]:
        try:
            response = self.ec2.describe_spot_price_history(
                InstanceTypes=self.config.instance_types,
                AvailabilityZones=self.config.availability_zones,
                ProductDescriptions=['Linux/UNIX'],
                MaxResults=100
            )
            return [float(r['SpotPrice']) for r in response['SpotPriceHistory']]
        except ClientError as e:
            logger.error(f"Price history error: {e.response['Error']['Message']}")
            return []

    def _calculate_optimal_price(self) -> str:
        if self.config.max_price == "on-demand-price":
            return self._get_ondemand_price()
        return self.config.max_price

    def _get_ondemand_price(self) -> str:
        try:
            response = self.ec2.describe_instance_types(
                InstanceTypes=self.config.instance_types
            )
            return str(max(
                float(i['Price']) for i in 
                response['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']
            ))
        except (KeyError, IndexError) as e:
            logger.error(f"On-demand price check failed: {str(e)}")
            return "0.10"  # Fallback default

    def request_spot_instances(self, count: int) -> List[str]:
        try:
            price = self._calculate_optimal_price()
            response = self.ec2.request_spot_instances(
                SpotPrice=price,
                InstanceCount=count,
                Type='persistent',
                AllocationStrategy=self.config.allocation_strategy,
                LaunchTemplate={
                    'LaunchTemplateName': self.config.launch_template
                },
                InstanceInterruptionBehavior='stop'
            )
            request_ids = [r['SpotInstanceRequestId'] 
                         for r in response['SpotInstanceRequests']]
            logger.info(f"Spot requests created: {request_ids}")
            return request_ids
        except ClientError as e:
            logger.critical(f"Spot request failed: {e.response['Error']['Message']}")
            if self.config.fallback_to_ondemand:
                return self._launch_ondemand_instances(count)
            return []

    def _launch_ondemand_instances(self, count: int) -> List[str]:
        try:
            response = self.ec2.run_instances(
                MinCount=count,
                MaxCount=count,
                LaunchTemplate={'LaunchTemplateName': self.config.launch_template}
            )
            instance_ids = [i['InstanceId'] for i in response['Instances']]
            logger.info(f"Fallback on-demand instances: {instance_ids}")
            return instance_ids
        except ClientError as e:
            logger.error(f"Ondemand launch failed: {e.response['Error']['Message']}")
            return []

    def monitor_spot_requests(self):
        try:
            requests = self.ec2.describe_spot_instance_requests(
                Filters=[{'Name': 'state', 'Values': ['open', 'active']}]
            )
            for req in requests['SpotInstanceRequests']:
                self._handle_request_status(req)
        except ClientError as e:
            logger.error(f"Monitoring error: {e.response['Error']['Message']}")

    def _handle_request_status(self, request: Dict):
        req_id = request['SpotInstanceRequestId']
        status = request['Status']['Code']
        
        if status == 'marked-for-termination':
            self._handle_termination(request)
        elif status == 'fulfilled' and req_id not in self.active_instances:
            self._track_new_instance(request)

    def _track_new_instance(self, request: Dict):
        instance_id = request['InstanceId']
        self.active_instances[instance_id] = {
            'request_id': request['SpotInstanceRequestId'],
            'launch_time': request['CreateTime'],
            'interrupted': False
        }
        logger.info(f"Tracking new instance: {instance_id}")

    def _handle_termination(self, request: Dict):
        instance_id = request['InstanceId']
        if instance_id in self.termination_handled:
            return
            
        logger.warning(f"Instance {instance_id} marked for termination")
        self._drain_instance(instance_id)
        self._replace_instance(instance_id)
        self.termination_handled.add(instance_id)

    def _drain_instance(self, instance_id: str):
        logger.info(f"Draining instance {instance_id}")
        # Implement LB deregistration here
        # Example: elb.deregister_instances(InstanceIds=[instance_id])

    def _replace_instance(self, instance_id: str):
        logger.info(f"Replacing instance {instance_id}")
        self.active_instances.pop(instance_id, None)
        self.request_spot_instances(1)

    def cleanup_terminated(self):
        try:
            instances = self.ec2.describe_instances(
                InstanceIds=list(self.active_instances.keys()),
                Filters=[{'Name': 'instance-state-name', 'Values': ['terminated']}]
            )
            for res in instances['Reservations']:
                for inst in res['Instances']:
                    self.active_instances.pop(inst['InstanceId'], None)
        except ClientError as e:
            logger.error(f"Cleanup error: {e.response['Error']['Message']}")

    def get_active_instances(self) -> List[str]:
        return list(self.active_instances.keys())

### Enterprise Features ###

1. **Cost Optimization**
```python
_calculate_optimal_price()  # Auto price calculation
