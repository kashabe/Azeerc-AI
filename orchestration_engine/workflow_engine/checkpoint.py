# checkpoint.py
import logging
import os
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.feather as pf
import hashlib
import lzma
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from prometheus_client import Gauge, Histogram, Counter

logger = logging.getLogger("CheckpointManager")

#region Prometheus Metrics
CHECKPOINT_SIZE = Gauge("checkpoint_size_bytes", "Snapshot size in bytes")
SNAPSHOT_TIME = Histogram("checkpoint_snapshot_seconds", "Time to create snapshots")
RESTORE_TIME = Histogram("checkpoint_restore_seconds", "Time to restore state")
CHECKPOINT_FAILURES = Counter("checkpoint_failures_total", "Failure reasons", ["operation", "error_type"])
#endregion

@dataclass(frozen=True)
class CheckpointMetadata:
    version: str
    timestamp: datetime
    checksum: str
    schema_hash: str
    compressed: bool

class CheckpointManager:
    """Enterprise-grade state snapshot manager using Apache Arrow"""
    
    def __init__(
        self,
        storage_path: str = "./checkpoints",
        compression: str = "lz4",
        max_retained: int = 5
    ):
        self._storage_path = Path(storage_path)
        self._compression = compression
        self._max_retained = max_retained
        self._lock = threading.RLock()
        self._storage_path.mkdir(parents=True, exist_ok=True)
        
        # Schema cache for validation
        self._schema_cache: Dict[str, pa.Schema] = {}

    def create_checkpoint(self, state: Any, version: str) -> CheckpointMetadata:
        """Serialize and compress system state with atomic write"""
        with SNAPSHOT_TIME.time(), self._lock:
            try:
                # Convert to Arrow Table
                if isinstance(state, pa.Table):
                    table = state
                else:
                    table = self._python_to_arrow(state)
                
                # Generate metadata
                checksum = self._compute_checksum(table)
                schema_hash = hashlib.sha256(str(table.schema).encode()).hexdigest()
                meta = CheckpointMetadata(
                    version=version,
                    timestamp=datetime.utcnow(),
                    checksum=checksum,
                    schema_hash=schema_hash,
                    compressed=self._compression != "none"
                )
                
                # Write with atomic replace
                tmp_path = self._storage_path / f"{version}.arrow.tmp"
                final_path = self._storage_path / f"{version}.arrow"
                
                with pa.OSFile(str(tmp_path), 'wb') as sink:
                    with pa.RecordBatchFileWriter(
                        sink, table.schema,
                        options=pa.ipc.IpcWriteOptions(compression=self._compression)
                    ) as writer:
                        writer.write_table(table)
                
                # Atomic move and register metadata
                os.rename(tmp_path, final_path)
                self._save_metadata(meta, final_path)
                self._cleanup_old_checkpoints()
                
                CHECKPOINT_SIZE.set(final_path.stat().st_size)
                return meta
                
            except Exception as e:
                CHECKPOINT_FAILURES.labels("create", type(e).__name__).inc()
                logger.error(f"Checkpoint failed: {str(e)}")
                raise

    def restore_checkpoint(self, version: str) -> pa.Table:
        """Load and validate checkpoint data with schema verification"""
        with RESTORE_TIME.time(), self._lock:
            try:
                path = self._storage_path / f"{version}.arrow"
                meta = self._load_metadata(path)
                
                with pa.memory_map(str(path), 'rb') as source:
                    reader = pa.ipc.open_file(source)
                    table = reader.read_all()
                
                # Validate integrity
                if meta.checksum != self._compute_checksum(table):
                    raise ValueError("Checksum mismatch")
                if meta.schema_hash != hashlib.sha256(str(table.schema).encode()).hexdigest():
                    raise ValueError("Schema drift detected")
                
                return table
                
            except Exception as e:
                CHECKPOINT_FAILURES.labels("restore", type(e).__name__).inc()
                logger.error(f"Restore failed: {str(e)}")
                raise

    def _python_to_arrow(self, data: Any) -> pa.Table:
        """Convert common Python types to Arrow Table"""
        if isinstance(data, dict):
            return pa.Table.from_pydict(data)
        elif isinstance(data, list):
            return pa.Table.from_pylist(data)
        elif isinstance(data, pa.Table):
            return data
        else:
            raise TypeError(f"Unsupported state type: {type(data)}")

    def _compute_checksum(self, table: pa.Table) -> str:
        """Compute XXH3 checksum for table data"""
        hasher = hashlib.blake2b()
        for column in table.columns:
            buf = column.data.buffers()[-1]  # Get values buffer
            hasher.update(buf.to_pybytes())
        return hasher.hexdigest()

    def _save_metadata(self, meta: CheckpointMetadata, path: Path) -> None:
        """Write metadata sidecar file"""
        meta_path = path.with_suffix(".meta")
        with open(meta_path, 'w') as f:
            f.write(f"version={meta.version}\n")
            f.write(f"timestamp={meta.timestamp.isoformat()}\n")
            f.write(f"checksum={meta.checksum}\n")
            f.write(f"schema_hash={meta.schema_hash}\n")
            f.write(f"compressed={meta.compressed}\n")

    def _load_metadata(self, path: Path) -> CheckpointMetadata:
        """Read metadata from sidecar file"""
        meta_path = path.with_suffix(".meta")
        if not meta_path.exists():
            raise FileNotFoundError(f"Metadata missing for {path.name}")
            
        with open(meta_path, 'r') as f:
            lines = f.readlines()
            params = dict(line.strip().split('=') for line in lines)
            
        return CheckpointMetadata(
            version=params['version'],
            timestamp=datetime.fromisoformat(params['timestamp']),
            checksum=params['checksum'],
            schema_hash=params['schema_hash'],
            compressed=params['compressed'].lower() == 'true'
        )

    def _cleanup_old_checkpoints(self) -> None:
        """Rotate checkpoints based on retention policy"""
        checkpoints = sorted(
            self._storage_path.glob("*.arrow"),
            key=os.path.getmtime,
            reverse=True
        )
        
        for old_checkpoint in checkpoints[self._max_retained:]:
            old_checkpoint.unlink()
            meta_path = old_checkpoint.with_suffix(".meta")
            if meta_path.exists():
                meta_path.unlink()

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Create sample state
    state_data = {
        "agent_ids": [f"agent_{i}" for i in range(1000)],
        "task_counts": [i % 10 for i in range(1000)],
        "last_heartbeat": [datetime.utcnow().isoformat() for _ in range(1000)]
    }
    
    manager = CheckpointManager(compression="zstd")
    metadata = manager.create_checkpoint(state_data, "v1.0.0")
    print(f"Checkpoint created: {metadata}")
    
    restored_table = manager.restore_checkpoint("v1.0.0")
    print(f"Restored table schema: {restored_table.schema}")
