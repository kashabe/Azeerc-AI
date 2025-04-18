# milvus_client.py
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
    MilvusClient,
    MilvusException,
    Hit
)

logger = logging.getLogger(__name__)

@dataclass
class VectorSearchConfig:
    host: str = "localhost"
    port: int = 19530
    user: str = ""
    password: str = ""
    pool_size: int = 10
    timeout: int = 30
    index_params: Dict = None
    search_params: Dict = None

class MilvusVectorClient:
    _DEFAULT_INDEX = {
        "IVF_FLAT": {"metric_type": "L2", "params": {"nlist": 16384}},
        "HNSW": {"metric_type": "L2", "params": {"M": 16, "efConstruction": 200}}
    }
    
    _DEFAULT_SEARCH = {
        "IVF_FLAT": {"params": {"nprobe": 128}},
        "HNSW": {"params": {"ef": 200}}
    }

    def __init__(self, config: VectorSearchConfig):
        self.config = config
        self._init_connection_pool()
        self._active_connections = set()
        self._collection_cache = {}
        self._setup_default_params()

    def _setup_default_params(self):
        self.config.index_params = self.config.index_params or self._DEFAULT_INDEX
        self.config.search_params = self.config.search_params or self._DEFAULT_SEARCH

    def _init_connection_pool(self):
        for i in range(self.config.pool_size):
            alias = f"conn_{i}"
            try:
                connections.connect(
                    alias=alias,
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    secure=False
                )
                self._active_connections.add(alias)
            except MilvusException as e:
                logger.error(f"Connection {alias} failed: {str(e)}")

    @contextmanager
    def _get_connection(self):
        alias = self._active_connections.pop() if self._active_connections else None
        if not alias:
            alias = next(iter(connections.list_connections()))
        
        try:
            yield alias
        finally:
            self._active_connections.add(alias)

    def _retry(func):
        def wrapper(self, *args, **kwargs):
            attempts = 3
            for i in range(attempts):
                try:
                    return func(self, *args, **kwargs)
                except MilvusException as e:
                    if i == attempts -1:
                        raise
                    logger.warning(f"Retry {i+1}/{attempts} for {func.__name__}")
                    self._init_connection_pool()
                    time.sleep(0.1 * (i+1))
        return wrapper

    @_retry
    def create_collection(self, name: str, dim: int, auto_id: bool=True) -> Collection:
        if utility.has_collection(name):
            return Collection(name)
            
        fields = [
            FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=64),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
            FieldSchema(name="metadata", dtype=DataType.JSON)
        ]
        
        schema = CollectionSchema(fields, description=f"{name} collection")
        collection = Collection(name, schema, consistency_level="Strong")
        
        # Create default index
        index_type = next(iter(self.config.index_params))
        collection.create_index(
            field_name="embedding",
            index_params=self.config.index_params[index_type]
        )
        
        return collection

    @_retry
    def insert_vectors(
        self,
        collection_name: str,
        ids: List[str],
        embeddings: np.ndarray,
        metadatas: List[Dict] = None
    ) -> List[str]:
        if not metadatas:
            metadatas = [{}] * len(ids)
            
        data = [
            ids,
            embeddings.tolist(),
            metadatas
        ]
        
        collection = self._get_collection(collection_name)
        result = collection.insert(data)
        collection.flush()
        return result.primary_keys

    @_retry
    def similarity_search(
        self,
        collection_name: str,
        query_embeddings: np.ndarray,
        top_k: int = 10,
        filter: Dict = None,
        **kwargs
    ) -> List[List[Hit]]:
        collection = self._get_collection(collection_name)
        search_params = self._get_search_params(collection)
        
        results = collection.search(
            data=query_embeddings.tolist(),
            anns_field="embedding",
            param=search_params,
            limit=top_k,
            expr=self._build_filter_expression(filter),
            output_fields=["metadata"],
            **kwargs
        )
        return results

    def _get_collection(self, name: str) -> Collection:
        if name not in self._collection_cache:
            self._collection_cache[name] = Collection(name)
        return self._collection_cache[name]

    def _build_filter_expression(self, filter: Dict) -> str:
        if not filter:
            return ""
            
        expressions = []
        for key, value in filter.items():
            if isinstance(value, list):
                expr = f"{key} in {value}"
            else:
                expr = f"{key} == {repr(value)}"
            expressions.append(expr)
            
        return " and ".join(expressions)

    def _get_search_params(self, collection: Collection) -> Dict:
        index_type = collection.index().params['index_type']
        return self.config.search_params.get(index_type, {"params": {}})

    @_retry
    def delete_vectors(self, collection_name: str, ids: List[str]) -> None:
        expr = f"id in {ids}"
        collection = self._get_collection(collection_name)
        collection.delete(expr)

    @_retry
    def get_collection_stats(self, name: str) -> Dict:
        return utility.get_collection_stats(name)

    @_retry
    def create_partition(self, collection_name: str, partition_name: str) -> None:
        collection = self._get_collection(collection_name)
        return collection.create_partition(partition_name)

    @_retry
    def backup_collection(self, name: str, path: str) -> None:
        utility.create_collection_backup(name, path)

    @_retry
    def restore_collection(self, backup_path: str, new_name: str = None) -> str:
        return utility.restore_collection_backup(backup_path, new_name)

    def close(self):
        for alias in connections.list_connections():
            connections.disconnect(alias)

### Enterprise Features ###

1. **Connection Pooling**
```python
_init_connection_pool()  # Manages multiple connections
