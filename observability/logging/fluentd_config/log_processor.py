# log_processor.py
import json
import logging
import re
import time
from collections import deque
from datetime import datetime
from statistics import mean, stdev
from typing import Dict, List, Optional, Tuple

import numpy as np
from isolation_forest import IsolationForest
from prometheus_client import Gauge

class LogProcessor:
    """Enterprise-grade log anomaly detection with streaming capability"""
    
    def __init__(
        self,
        window_size: int = 300,
        thresholds: Dict[str, float] = None,
        methods: List[str] = ["statistical", "isolation_forest"]
    ):
        self.window_size = window_size  # Seconds
        self.methods = methods
        self.thresholds = thresholds or {
            "error_rate": 0.05,
            "frequency": 1000,
            "novelty": -0.5
        }
        
        # Data structures
        self.log_buffer = deque(maxlen=1_000_000)
        self.error_counts = deque(maxlen=window_size)
        self.log_patterns = {}
        
        # ML Models
        if "isolation_forest" in methods:
            self.iforest = IsolationForest(n_estimators=100)
            self.feature_matrix = []
            
        # Monitoring
        self.anomaly_gauge = Gauge(
            "log_anomalies_total", 
            "Total detected log anomalies",
            ["method", "severity"]
        )
        
        # Performance tracking
        self._last_processed = time.time()
        self._lock = threading.RLock()

    def ingest_log(self, raw_log: str):
        """Thread-safe log ingestion with pattern extraction"""
        with self._lock:
            timestamp = time.time()
            parsed = self._parse_log(raw_log)
            self.log_buffer.append((timestamp, parsed))
            
            # Update error rates
            if parsed["level"] == "ERROR":
                self.error_counts.append(timestamp)
                
            # Update pattern frequencies
            signature = self._create_signature(parsed["message"])
            self.log_patterns[signature] = \
                self.log_patterns.get(signature, 0) + 1

    def _parse_log(self, raw_log: str) -> Dict:
        """Parse structured/unstructured logs with auto-format detection"""
        try:
            # Try JSON parsing first
            log_entry = json.loads(raw_log)
            return {
                "timestamp": log_entry.get("timestamp", time.time()),
                "level": log_entry.get("level", "INFO"),
                "message": log_entry["message"],
                "context": log_entry.get("context", {})
            }
        except json.JSONDecodeError:
            # Fallback to regex parsing
            return self._parse_text_log(raw_log)

    def _parse_text_log(self, raw_log: str) -> Dict:
        """Handle common log formats (CLF/ELF)"""
        clf_pattern = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+)'
        match = re.match(clf_pattern, raw_log)
        if match:
            return {
                "timestamp": time.time(),
                "level": "INFO",
                "message": f"{match.group(5)} {match.group(6)}",
                "context": {
                    "client": match.group(1),
                    "status": match.group(8)
                }
            }
        return {"timestamp": time.time(), "level": "INFO", "message": raw_log}

    def _create_signature(self, message: str) -> str:
        """Convert log messages to pattern signatures"""
        cleaned = re.sub(r'\d+', '<NUM>', message)
        cleaned = re.sub(r'0x[0-9a-f]+', '<HEX>', cleaned)
        return re.sub(r'\b\w{32,}\b', '<HASH>', cleaned)

    def detect_anomalies(self) -> List[Dict]:
        """Multi-method anomaly detection pipeline"""
        anomalies = []
        current_time = time.time()
        
        with self._lock:
            # Statistical analysis
            if "statistical" in self.methods:
                stats_anomalies = self._statistical_analysis(current_time)
                anomalies.extend(stats_anomalies)
                
            # ML-based detection
            if "isolation_forest" in self.methods:
                ml_anomalies = self._ml_analysis(current_time)
                anomalies.extend(ml_anomalies)
                
        return anomalies

    def _statistical_analysis(self, current_time: float) -> List[Dict]:
        """Real-time statistical anomaly detection"""
        anomalies = []
        
        # Error rate check
        error_count = sum(
            1 for ts in self.error_counts 
            if current_time - ts <= self.window_size
        )
        error_rate = error_count / len(self.log_buffer) if self.log_buffer else 0
        if error_rate > self.thresholds["error_rate"]:
            anomalies.append({
                "method": "statistical",
                "type": "error_rate",
                "value": error_rate,
                "threshold": self.thresholds["error_rate"]
            })
            self.anomaly_gauge.labels("statistical", "error").inc()

        # Frequency analysis
        freq_anomalies = [
            sig for sig, count in self.log_patterns.items()
            if count > self.thresholds["frequency"]
        ]
        for sig in freq_anomalies:
            anomalies.append({
                "method": "statistical",
                "type": "frequency",
                "pattern": sig,
                "count": self.log_patterns[sig]
            })
            self.anomaly_gauge.labels("statistical", "frequency").inc()

        return anomalies

    def _ml_analysis(self, current_time: float) -> List[Dict]:
        """Machine learning based anomaly detection"""
        features = self._extract_features()
        if not features:
            return []
            
        # Train/update model incrementally
        self.iforest.fit(features)
        
        # Predict anomalies
        scores = self.iforest.decision_function(features)
        anomalies = []
        for i, score in enumerate(scores):
            if score < self.thresholds["novelty"]:
                anomalies.append({
                    "method": "isolation_forest",
                    "score": score,
                    "example": self.log_buffer[i][1]["message"]
                })
                self.anomaly_gauge.labels("ml", "novelty").inc()
                
        return anomalies

    def _extract_features(self) -> np.ndarray:
        """Feature engineering for ML models"""
        # Extract 1. Message length 2. Word count 3. Special char count
        # 4. Log level 5. Time since last log
        features = []
        window_logs = [
            log for ts, log in self.log_buffer
            if time.time() - ts <= self.window_size
        ]
        
        for log in window_logs[-1000:]:  # Use last 1000 logs for training
            msg = log["message"]
            features.append([
                len(msg),
                len(msg.split()),
                sum(1 for c in msg if not c.isalnum()),
                {"DEBUG":0, "INFO":1, "WARN":2, "ERROR":3}.get(log["level"],4),
                time.time() - log["timestamp"]
            ])
            
        return np.array(features)

    def stream_processing(self):
        """Real-time processing loop"""
        while True:
            try:
                anomalies = self.detect_anomalies()
                if anomalies:
                    self._trigger_alerts(anomalies)
                time.sleep(1)
            except Exception as e:
                logging.error(f"Processing error: {str(e)}")

    def _trigger_alerts(self, anomalies: List[Dict]):
        """Integrate with alerting systems"""
        critical_anomalies = [
            a for a in anomalies 
            if a["method"] == "statistical" and a["type"] == "error_rate"
        ]
        if critical_anomalies:
            self._pagerduty_alert(critical_anomalies)

    def _pagerduty_alert(self, anomalies: List[Dict]):
        """Send critical alerts to PagerDuty"""
        import requests
        requests.post(
            "https://events.pagerduty.com/v2/enqueue",
            json={
                "routing_key": os.getenv("PAGERDUTY_KEY"),
                "event_action": "trigger",
                "payload": {
                    "summary": f"{len(anomalies)} log anomalies detected",
                    "severity": "critical",
                    "source": "AzeercAI-LogProcessor",
                    "custom_details": anomalies
                }
            }
        )

# Enterprise Features

1. **Autoencoder Integration**
```python
from tensorflow.keras.models import Model, Sequential
from tensorflow.keras.layers import Dense

class AutoencoderAnomalyDetector:
    """Deep learning based anomaly detection"""
    
    def __init__(self, input_dim: int):
        self.model = self._build_model(input_dim)
        
    def _build_model(self, input_dim: int) -> Model:
        model = Sequential([
            Dense(32, activation='relu', input_shape=(input_dim,)),
            Dense(8, activation='relu'),
            Dense(32, activation='relu'),
            Dense(input_dim, activation='linear')
        ])
        model.compile(optimizer='adam', loss='mse')
        return model
        
    def detect(self, features: np.ndarray, threshold: float) -> np.ndarray:
        reconstructions = self.model.predict(features)
        loss = np.mean(np.square(features - reconstructions), axis=1)
        return loss > threshold
