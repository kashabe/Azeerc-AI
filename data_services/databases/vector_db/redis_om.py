# redis_om.py
import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional, Type, TypeVar

import redis
from redis.commands.search.field import NumericField, TagField, TextField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

logger = logging.getLogger(__name__)
T = TypeVar("T")

class RedisConnectionPool:
    _pool = None
    
    @classmethod
    def init_pool(cls, host: str, port: int, password: str, db: int=0, max_connections: int=20):
        cls._pool = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,
            max_connections=max_connections,
            socket_timeout=5,
            decode_responses=False
        )
    
    @classmethod
    @contextmanager
    def get_connection(cls) -> Generator[redis.Redis, None, None]:
        if not cls._pool:
            raise RuntimeError("Connection pool not initialized")
            
        conn = redis.Redis(connection_pool=cls._pool)
        try:
            yield conn
        finally:
            conn.close()

@dataclass
class RedisModelConfig:
    host: str = "localhost"
    port: int = 6379
    password: str = ""
    db: int = 0
    index_prefix: str = "idx"
    cache_ttl: int = 3600
    connection_pool_size: int = 20

class RedisOM:
    _initialized = False
    
    def __init__(self, config: RedisModelConfig):
        self.config = config
        self._init_connection_pool()
        self.__class__._initialized = True
    
    def _init_connection_pool(self):
        if not self.__class__._initialized:
            RedisConnectionPool.init_pool(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                max_connections=self.config.connection_pool_size
            )
    
    @contextmanager
    def _get_conn(self):
        with RedisConnectionPool.get_connection() as conn:
            yield conn
    
    @staticmethod
    def _generate_id() -> str:
        return f"obj:{uuid.uuid4().hex}"
    
    def _key(self, model_cls: Type[T], id: str) -> str:
        return f"{model_cls.__name__.lower()}:{id}"
    
    def create_index(self, model_cls: Type[T], fields: List[str]) -> None:
        with self._get_conn() as conn:
            try:
                idx = conn.ft(f"{self.config.index_prefix}:{model_cls.__name__.lower()}")
                schema = []
                
                for field in fields:
                    if field.endswith("_num"):
                        schema.append(NumericField(field))
                    elif field.endswith("_tag"):
                        schema.append(TagField(field))
                    else:
                        schema.append(TextField(field))
                
                idx.create_index(
                    schema,
                    definition=IndexDefinition(
                        prefix=[self._key(model_cls, "")],
                        index_type=IndexType.JSON
                    )
                )
            except Exception as e:
                logger.error(f"Index creation failed: {str(e)}")
                raise
    
    def save(self, model: T, ttl: Optional[int] = None) -> T:
        with self._get_conn() as conn:
            if not getattr(model, "id", None):
                model.id = self._generate_id()
            
            key = self._key(type(model), model.id)
            data = json.dumps(model.__dict__)
            
            pipeline = conn.pipeline()
            pipeline.json().set(key, "$", data)
            if ttl or self.config.cache_ttl:
                pipeline.expire(key, ttl or self.config.cache_ttl)
            pipeline.execute()
            
            return model
    
    def get(self, model_cls: Type[T], id: str, cache_only: bool=False) -> Optional[T]:
        with self._get_conn() as conn:
            key = self._key(model_cls, id)
            data = conn.json().get(key)
            
            if data:
                obj = model_cls(**json.loads(data))
                obj.id = id
                return obj
            return None
    
    def search(self, model_cls: Type[T], query: str, limit: int=100, offset: int=0) -> List[T]:
        with self._get_conn() as conn:
            idx = conn.ft(f"{self.config.index_prefix}:{model_cls.__name__.lower()}")
            q = Query(query).paging(offset, limit)
            
            try:
                results = idx.search(q)
                return [self.get(model_cls, doc.id) for doc in results.docs]
            except Exception as e:
                logger.error(f"Search failed: {str(e)}")
                return []
    
    def delete(self, model_cls: Type[T], id: str) -> bool:
        with self._get_conn() as conn:
            key = self._key(model_cls, id)
            return bool(conn.delete(key))
    
    def atomic_update(self, model_cls: Type[T], id: str, updates: Dict[str, Any]) -> bool:
        with self._get_conn() as conn:
            key = self._key(model_cls, id)
            path_updates = {f"$.{k}": v for k,v in updates.items()}
            
            try:
                return conn.json().set(key, path_updates)
            except Exception as e:
                logger.error(f"Atomic update failed: {str(e)}")
                return False

class BaseModel:
    id: Optional[str] = None
    
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    
    @classmethod
    def _get_om(cls) -> RedisOM:
        return cls.Meta.om
    
    class Meta:
        om: RedisOM = None
        index_fields: List[str] = []
    
    def save(self, ttl: Optional[int] = None) -> "BaseModel":
        return self._get_om().save(self, ttl)
    
    @classmethod
    def get(cls, id: str) -> Optional["BaseModel"]:
        return cls._get_om().get(cls, id)
    
    @classmethod
    def search(cls, query: str, limit: int=100, offset: int=0) -> List["BaseModel"]:
        return cls._get_om().search(cls, query, limit, offset)
    
    @classmethod
    def delete(cls, id: str) -> bool:
        return cls._get_om().delete(cls, id)

### Enterprise Features ###

1. **Connection Pooling**
```python
RedisConnectionPool  # Manages connection lifecycle
