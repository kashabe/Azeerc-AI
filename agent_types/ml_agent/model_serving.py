# model_serving.py
import asyncio
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import onnxruntime as ort
import tensorrt as trt

TRT_LOGGER = trt.Logger(trt.Logger.ERROR)
ORT_SESS_OPTIONS = ort.SessionOptions()
ORT_SESS_OPTIONS.intra_op_num_threads = 1
ORT_SESS_OPTIONS.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL

LOG = logging.getLogger("ModelServer")

@dataclass(frozen=True)
class ModelConfig:
    model_path: Path
    precision: str = "fp16"
    max_batch_size: int = 32
    max_workers: int = 2
    warmup_iters: int = 10

class InferenceEngine:
    def __init__(self, config: ModelConfig):
        self.config = config
        self._backend = self._init_backend()
        self._request_queue = asyncio.Queue()
        self._executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self._is_serving = False
        self._batch_size = 1
        self._metrics = {
            "total_inferences": 0,
            "avg_latency": 0.0,
            "throughput": 0.0
        }
        self._warmup()

    def _init_backend(self) -> ort.InferenceSession | trt.IExecutionContext:
        """Auto-select inference backend based on hardware"""
        model_ext = self.config.model_path.suffix
        if model_ext == ".onnx":
            return ort.InferenceSession(
                str(self.config.model_path),
                sess_options=ORT_SESS_OPTIONS,
                providers=["CUDAExecutionProvider", "CPUExecutionProvider"]
            )
        elif model_ext == ".engine":
            with open(self.config.model_path, "rb") as f, trt.Runtime(TRT_LOGGER) as runtime:
                engine = runtime.deserialize_cuda_engine(f.read())
            context = engine.create_execution_context()
            return context
        else:
            raise ValueError(f"Unsupported model format: {model_ext}")

    def _warmup(self) -> None:
        """Warmup inference for stable performance"""
        dummy_input = self._create_dummy_input()
        for _ in range(self.config.warmup_iters):
            self._infer(dummy_input)
        LOG.info(f"Warmup completed for {self.config.model_path.name}")

    def _create_dummy_input(self) -> Dict[str, np.ndarray]:
        """Generate dummy input based on model specs"""
        if isinstance(self._backend, ort.InferenceSession):
            return {inp.name: np.random.rand(*inp.shape).astype(np.float32) 
                    for inp in self._backend.get_inputs()}
        else:  # TensorRT
            return {"input": np.random.rand(self._backend.get_binding_shape(0)).astype(np.float32)}

    async def enqueue_request(self, input_data: Dict[str, np.ndarray]) -> np.ndarray:
        """Async interface for inference requests"""
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        await self._request_queue.put((input_data, future))
        return await future

    async def start_serving(self) -> None:
        """Start dynamic batching loop"""
        self._is_serving = True
        LOG.info(f"Model server started for {self.config.model_path.name}")
        while self._is_serving:
            batch, futures = await self._collect_batch()
            if not batch:
                await asyncio.sleep(0.001)
                continue
            
            start_time = time.monotonic()
            output = await loop.run_in_executor(
                self._executor, self._infer, batch
            )
            latency = (time.monotonic() - start_time) * 1000  # ms
            
            self._update_metrics(latency, len(batch))
            self._dispatch_results(output, futures)

    async def _collect_batch(self) -> Tuple[List[Dict[str, np.ndarray]], List[asyncio.Future]]:
        """Gather requests for dynamic batching"""
        batch = []
        futures = []
        while len(batch) < self.config.max_batch_size and not self._request_queue.empty():
            data, future = await self._request_queue.get()
            batch.append(data)
            futures.append(future)
        return batch, futures

    def _infer(self, batch: List[Dict[str, np.ndarray]]) -> List[np.ndarray]:
        """Core inference logic"""
        if isinstance(self._backend, ort.InferenceSession):
            return self._onnx_inference(batch)
        else:
            return self._trt_inference(batch)

    def _onnx_inference(self, batch: List[Dict[str, np.ndarray]]) -> List[np.ndarray]:
        """Batch inference for ONNX models"""
        feed_dict = {k: np.concatenate([item[k] for item in batch]) 
                     for k in self._backend.get_inputs()[0].name}
        outputs = self._backend.run(None, feed_dict)
        return np.split(outputs[0], len(batch))

    def _trt_inference(self, batch: List[Dict[str, np.ndarray]]) -> List[np.ndarray]:
        """TensorRT inference with explicit batch"""
        # Allocate buffers
        inputs, outputs, bindings = [], [], []
        stream = trt.Runtime().create_cuda_stream()
        
        # Prepare batch
        batch_input = np.concatenate([item["input"] for item in batch])
        
        # Execute inference
        self._backend.execute_async_v2(
            bindings=[int(batch_input.ctypes.data)],
            stream_handle=stream
        )
        
        # Copy outputs
        return np.split(batch_input, len(batch))  # Simplified for demo

    def _update_metrics(self, latency: float, batch_size: int) -> None:
        """Update performance metrics"""
        self._metrics["total_inferences"] += batch_size
        self._metrics["avg_latency"] = (
            self._metrics["avg_latency"] * 0.9 + latency * 0.1
        )
        self._metrics["throughput"] = batch_size / (latency / 1000)

    def _dispatch_results(self, outputs: List[np.ndarray], futures: List[asyncio.Future]) -> None:
        """Complete async futures with results"""
        for output, future in zip(outputs, futures):
            future.set_result(output)

    def stop_serving(self) -> None:
        """Graceful shutdown"""
        self._is_serving = False
        self._executor.shutdown()
        LOG.info(f"Model server stopped for {self.config.model_path.name}")

    def health_check(self) -> Dict[str, float]:
        """Kubernetes-ready health check"""
        return {
            "status": "SERVING" if self._is_serving else "DOWN",
            **self._metrics
        }

class InferenceError(Exception):
    """Base exception for inference failures"""

class ModelVersionMismatchError(InferenceError):
    """Model version conflict"""

# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    config = ModelConfig(
        model_path=Path("models/resnet50.onnx"),
        max_batch_size=64,
        precision="fp16"
    )
    
    engine = InferenceEngine(config)
    loop = asyncio.get_event_loop()
    
    async def simulate_requests():
        tasks = []
        for _ in range(100):
            dummy_input = engine._create_dummy_input()
            tasks.append(engine.enqueue_request(dummy_input))
        await asyncio.gather(*tasks)
    
    try:
        loop.run_until_complete(asyncio.gather(
            engine.start_serving(),
            simulate_requests()
        ))
    finally:
        engine.stop_serving()
        loop.close()
