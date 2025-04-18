# compliance_checker.py
import datetime
import hashlib
import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
from cryptography.fernet import Fernet
from pydantic import BaseModel, ValidationError

logger = logging.getLogger("ComplianceChecker")

class ComplianceError(Exception):
    """Critical compliance violation"""

class EvidenceSource(ABC):
    @abstractmethod
    def fetch_evidence(self) -> Any:
        """Retrieve evidence from source system"""

class AuditLogSource(EvidenceSource):
    def __init__(self, api_endpoint: str):
        self.endpoint = api_endpoint
        
    def fetch_evidence(self) -> List[Dict[str, Any]]:
        response = requests.get(
            f"{self.endpoint}/audit-logs",
            params={"since": datetime.datetime.utcnow() - datetime.timedelta(days=365)},
            timeout=30
        )
        response.raise_for_status()
        return response.json()["logs"]

class ComplianceRule(ABC):
    @abstractmethod
    def evaluate(self, evidence: Any) -> Tuple[bool, Dict[str, Any]]:
        """Return (compliance_status, detailed_report)"""

class GDPRDataMinimizationRule(ComplianceRule):
    def evaluate(self, evidence: List[Dict]) -> Tuple[bool, Dict]:
        report = {"passed": True, "violations": []}
        for data_entry in evidence:
            if len(data_entry.get("personal_data_fields", [])) > 15:
                report["passed"] = False
                report["violations"].append({
                    "id": data_entry["id"],
                    "reason": "Excessive personal data collection",
                    "article": "GDPR Article 5(1)(c)"
                })
        return report["passed"], report

class HIPAAContentEncryptionRule(ComplianceRule):
    def evaluate(self, configs: Dict) -> Tuple[bool, Dict]:
        report = {"passed": True, "issues": []}
        if configs["storage"]["encryption"]["algorithm"] != "AES-256":
            report["passed"] = False
            report["issues"].append("Insufficient encryption standard")
        if configs["storage"]["encryption"]["key_rotation_days"] > 90:
            report["passed"] = False
            report["issues"].append("Key rotation period exceeds 90 days")
        return report["passed"], report

class ComplianceReport(BaseModel):
    framework: str
    assessment_date: datetime.datetime
    overall_status: bool
    rules_passed: int
    rules_failed: int
    detailed_results: Dict[str, Any]
    evidence_hash: str

class ComplianceChecker:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.rules = self._init_rules()
        self.sources = self._init_evidence_sources()

    def _load_config(self, path: str) -> Dict:
        with open(path) as f:
            return json.load(f)

    def _init_rules(self) -> Dict[str, ComplianceRule]:
        return {
            "GDPR_ART5_DATA_MINIMIZATION": GDPRDataMinimizationRule(),
            "HIPAA_164_312_AES256": HIPAAContentEncryptionRule(),
        }

    def _init_evidence_sources(self) -> Dict[str, EvidenceSource]:
        return {
            "audit_logs": AuditLogSource(self.config["audit_log_endpoint"]),
            "config_db": DatabaseConfigSource(self.config["db_uri"])
        }

    def generate_report(self) -> ComplianceReport:
        evidence = self._collect_evidence()
        results = {}
        
        for rule_id, rule in self.rules.items():
            try:
                passed, details = rule.evaluate(evidence)
                results[rule_id] = {
                    "passed": passed,
                    "details": details
                }
            except Exception as e:
                logger.error(f"Rule {rule_id} evaluation failed: {str(e)}")
                results[rule_id] = {"error": str(e)}

        return ComplianceReport(
            framework="GDPR/HIPAA",
            assessment_date=datetime.datetime.utcnow(),
            overall_status=all(r["passed"] for r in results.values() if "passed" in r),
            rules_passed=sum(1 for r in results.values() if r.get("passed")),
            rules_failed=sum(1 for r in results.values() if not r.get("passed")),
            detailed_results=results,
            evidence_hash=self._hash_evidence(evidence)
        )

    def _collect_evidence(self) -> Dict[str, Any]:
        evidence = {}
        for source_name, source in self.sources.items():
            try:
                evidence[source_name] = source.fetch_evidence()
            except Exception as e:
                logger.critical(f"Evidence collection failed for {source_name}: {str(e)}")
                raise ComplianceError("Evidence collection failure")
        return evidence

    def _hash_evidence(self, evidence: Dict) -> str:
        return hashlib.sha256(json.dumps(evidence).encode()).hexdigest()

if __name__ == "__main__":
    checker = ComplianceChecker("config/compliance_config.json")
    report = checker.generate_report()
    print(json.dumps(report.dict(), indent=2, default=str))
