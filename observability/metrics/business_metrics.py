# business_metrics.py
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import mean
from typing import Deque, Dict, List, Optional, Tuple

class SLATracker:
    """Enterprise-grade SLA monitoring with dynamic thresholds"""
    
    def __init__(
        self,
        service_name: str,
        targets: Dict[str, float],
        window_size: int = 300
    ):
        self.service_name = service_name
        self.targets = targets  # {"availability": 99.9, "latency_p99": 0.2}
        self.window_size = window_size  # Seconds
        self.metrics: Dict[str, Deque[Tuple[float, float]]] = {
            "requests": deque(maxlen=10_000),
            "errors": deque(maxlen=10_000),
            "latencies": deque(maxlen=10_000)
        }
        self._lock = threading.RLock()
        
    def record_request(
        self,
        success: bool,
        latency: float,
        timestamp: Optional[float] = None
    ):
        """Thread-safe metric recording with nanosecond precision"""
        ts = timestamp or time.time_ns() / 1e9
        with self._lock:
            self.metrics["requests"].append((ts, 1.0))
            if not success:
                self.metrics["errors"].append((ts, 1.0))
            self.metrics["latencies"].append((ts, latency))

    def _calculate_throughput(self, window: int) -> float:
        """Requests per second in specified time window"""
        now = time.time()
        return sum(
            1 for ts, _ in self.metrics["requests"]
            if now - ts <= window
        ) / window

    def _calculate_availability(self, window: int) -> float:
        """Successful request percentage in time window"""
        now = time.time()
        total = sum(
            1 for ts, _ in self.metrics["requests"]
            if now - ts <= window
        )
        errors = sum(
            1 for ts, _ in self.metrics["errors"]
            if now - ts <= window
        )
        return ((total - errors) / total * 100) if total else 100.0

    def _calculate_latency_quantile(self, window: int, quantile: float) -> float:
        """Compute latency quantile within time window"""
        now = time.time()
        latencies = sorted(
            [lat for ts, lat in self.metrics["latencies"]
             if now - ts <= window],
            reverse=True
        )
        if not latencies:
            return 0.0
        index = int(len(latencies) * (1 - quantile))
        return latencies[index]

    def check_sla_compliance(self) -> Dict[str, Tuple[float, float, bool]]:
        """Evaluate all SLA targets against current performance"""
        compliance = {}
        window = self.window_size
        
        with self._lock:
            availability = self._calculate_availability(window)
            latency_p99 = self._calculate_latency_quantile(window, 0.99)
            throughput = self._calculate_throughput(window)
            
            compliance["availability"] = (
                availability, 
                self.targets["availability"],
                availability >= self.targets["availability"]
            )
            
            compliance["latency_p99"] = (
                latency_p99,
                self.targets["latency_p99"],
                latency_p99 <= self.targets["latency_p99"]
            )
            
            if "throughput" in self.targets:
                compliance["throughput"] = (
                    throughput,
                    self.targets["throughput"],
                    throughput >= self.targets["throughput"]
                )
        
        return compliance

    def generate_sla_report(
        self,
        granularity: str = "5min",
        time_range: str = "1h"
    ) -> Dict:
        """Generate detailed SLA compliance report"""
        report = {
            "service": self.service_name,
            "time_range": time_range,
            "granularity": granularity,
            "slices": []
        }
        
        now = datetime.utcnow()
        window_sec = {
            "1min": 60,
            "5min": 300,
            "1h": 3600
        }[granularity]
        
        for i in range(int(time_range[:-1]) * 60 // window_sec):
            slice_start = now - timedelta(seconds=window_sec*(i+1))
            slice_end = now - timedelta(seconds=window_sec*i)
            
            availability = self._calculate_availability(window_sec)
            latency_p99 = self._calculate_latency_quantile(window_sec, 0.99)
            
            report["slices"].append({
                "start": slice_start.isoformat(),
                "end": slice_end.isoformat(),
                "availability_pct": availability,
                "latency_p99": latency_p99,
                "throughput": self._calculate_throughput(window_sec)
            })
            
        return report

class ThroughputAnalyzer:
    """Real-time throughput analysis with anomaly detection"""
    
    def __init__(self, sample_window: int = 60, history_size: int = 1440):
        self.samples = deque(maxlen=history_size)
        self.window = sample_window
        self._baseline = None
        
    def update_throughput(self, rps: float):
        """Add new throughput sample"""
        self.samples.append((datetime.utcnow(), rps))
        
    def detect_anomalies(self, sensitivity: float = 3.0) -> List[Dict]:
        """Identify throughput deviations using Z-score"""
        if len(self.samples) < 30:
            return []
            
        values = [rps for _, rps in self.samples]
        mean_rps = mean(values)
        std_dev = (sum((x - mean_rps)**2 for x in values)/len(values))**0.5
        
        anomalies = []
        for ts, rps in self.samples:
            z_score = (rps - mean_rps) / std_dev if std_dev != 0 else 0
            if abs(z_score) > sensitivity:
                anomalies.append({
                    "timestamp": ts,
                    "throughput": rps,
                    "z_score": z_score,
                    "status": "high" if z_score > 0 else "low"
                })
                
        return anomalies

# Example Usage
if __name__ == "__main__":
    # Initialize SLA tracker for NLP service
    nlp_sla = SLATracker(
        service_name="nlp-api",
        targets={
            "availability": 99.95,
            "latency_p99": 0.5,  # 500ms p99 latency
            "throughput": 1000   # 1000 RPS
        }
    )
    
    # Simulate metrics
    for i in range(1000):
        success = i % 100 != 0  # 99% success rate
        latency = 0.1 + (0.5 if not success else 0) + random.random()/10
        nlp_sla.record_request(success, latency)
        time.sleep(0.001)
        
    # Check real-time compliance
    compliance = nlp_sla.check_sla_compliance()
    print(f"SLA Compliance: {compliance}")
    
    # Generate hourly report
    report = nlp_sla.generate_sla_report(granularity="5min", time_range="1h")
    print(f"Hourly Report: {report}")

# Enterprise Features

1. **Data Persistence**
```python
def save_to_tsdb(self):
    """Write metrics to InfluxDB"""
    from influxdb_client import InfluxDBClient
    client = InfluxDBClient(url="http://monitoring:8086", token="***")
    with client.write_api() as writer:
        writer.write("azeerc_metrics", record={
            "measurement": self.service_name,
            "tags": {"env": "prod"},
            "fields": {
                "availability": self._calculate_availability(self.window_size),
                "latency_p99": self._calculate_latency_quantile(self.window_size, 0.99)
            }
        })
