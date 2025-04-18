# state_manager.py
from __future__ import annotations
import logging
import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any
import redis
import asyncpg

# Custom Exceptions
class StateManagerError(Exception):
    """Base exception for state management operations."""
    pass

class ConnectionError(StateManagerError):
    """Failed to connect to state storage backend."""
    pass

class SerializationError(StateManagerError):
    """Data serialization/deserialization failure."""
    pass

class NotFoundError(StateManagerError):
    """Requested state record not found."""
    pass

class VersionConflictError(StateManagerError):
    """Optimistic locking version conflict."""
    pass

@dataclass
class AgentState:
    agent_id: str
    status: str
    last_updated: datetime
    metadata: Dict[str, Any]
    version: int = 0

class IStateManager(ABC):
    """Abstract interface for state persistence."""
    
    @abstractmethod
    def save_state(self, state: AgentState) -> AgentState:
        pass
    
    @abstractmethod
    def load_state(self, agent_id: str) -> Optional[AgentState]:
        pass
    
    @abstractmethod
    def delete_state(self, agent_id: str) -> None:
        pass
    
    @abstractmethod
    def exists(self, agent_id: str) -> bool:
        pass
    
    @abstractmethod
    def health_check(self) -> bool:
        pass

class RedisStateManager(IStateManager):
    """Redis-based state implementation with connection pooling."""
    
    def __init__(self, 
                 host: str = 'localhost', 
                 port: int = 6379,
                 db: int = 0,
                 key_prefix: str = "azeerc:state"):
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=2,
            retry_on_timeout=True
        )
        self.key_prefix = key_prefix
        self.logger = logging.getLogger("RedisStateManager")
        
    def _get_key(self, agent_id: str) -> str:
        return f"{self.key_prefix}:{agent_id}"
    
    def save_state(self, state: AgentState) -> AgentState:
        """Save state with optimistic locking using versioning."""
        key = self._get_key(state.agent_id)
        pipe = redis.Redis(connection_pool=self.pool).pipeline()
        
        try:
            pipe.watch(key)
            if pipe.exists(key):
                existing_version = pipe.hget(key, 'version')
                if int(existing_version) != state.version:
                    raise VersionConflictError(f"Version conflict for agent {state.agent_id}")
            
            state.version += 1
            state_data = asdict(state)
            state_data['last_updated'] = state_data['last_updated'].isoformat()
            
            pipe.multi()
            pipe.hset(key, mapping={
                'status': state.status,
                'last_updated': state_data['last_updated'],
                'metadata': json.dumps(state.metadata),
                'version': state.version
            })
            pipe.execute()
            return state
            
        except redis.exceptions.WatchError:
            raise VersionConflictError(f"Concurrent modification detected for {state.agent_id}")
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            self.logger.error(f"Redis connection failed: {str(e)}")
            raise ConnectionError("Redis connection failure") from e
        except json.JSONDecodeError as e:
            self.logger.error(f"Serialization error: {str(e)}")
            raise SerializationError("Metadata serialization failed") from e
        finally:
            pipe.reset()
    
    def load_state(self, agent_id: str) -> Optional[AgentState]:
        """Load state with automatic deserialization."""
        key = self._get_key(agent_id)
        client = redis.Redis(connection_pool=self.pool)
        
        try:
            if not client.exists(key):
                return None
                
            data = client.hgetall(key)
            return AgentState(
                agent_id=agent_id,
                status=data['status'],
                last_updated=datetime.fromisoformat(data['last_updated']),
                metadata=json.loads(data['metadata']),
                version=int(data['version'])
            )
        except KeyError as e:
            self.logger.error(f"Missing field in Redis data: {str(e)}")
            raise SerializationError("Incomplete state data") from e
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            self.logger.error(f"Redis connection failed: {str(e)}")
            raise ConnectionError("Redis connection failure") from e
        except json.JSONDecodeError as e:
            self.logger.error(f"Deserialization error: {str(e)}")
            raise SerializationError("Metadata deserialization failed") from e
    
    def delete_state(self, agent_id: str) -> None:
        key = self._get_key(agent_id)
        client = redis.Redis(connection_pool=self.pool)
        if not client.delete(key):
            raise NotFoundError(f"State not found for agent {agent_id}")
    
    def exists(self, agent_id: str) -> bool:
        key = self._get_key(agent_id)
        client = redis.Redis(connection_pool=self.pool)
        return client.exists(key) == 1
    
    def health_check(self) -> bool:
        try:
            client = redis.Redis(connection_pool=self.pool)
            return client.ping()
        except redis.exceptions.RedisError:
            return False

class PostgresStateManager(IStateManager):
    """PostgreSQL-based state implementation with async support."""
    
    def __init__(self,
                 dsn: str = "postgresql://user:pass@localhost:5432/azeerc",
                 table_name: str = "agent_states"):
        self.dsn = dsn
        self.table_name = table_name
        self.pool = None
        self.logger = logging.getLogger("PostgresStateManager")
    
    async def initialize(self):
        """Initialize connection pool (must be called before use)."""
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=2,
            max_size=10,
            command_timeout=5,
            server_settings={'application_name': 'AzeercStateManager'}
        )
        
        async with self.pool.acquire() as conn:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    agent_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_updated TIMESTAMPTZ NOT NULL,
                    metadata JSONB NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0
                )
            """)
    
    async def save_state(self, state: AgentState) -> AgentState:
        """Upsert state with version-based optimistic locking."""
        if not self.pool:
            raise ConnectionError("Connection pool not initialized")
            
        query = f"""
            INSERT INTO {self.table_name} 
            (agent_id, status, last_updated, metadata, version)
            VALUES (\$1, \$2, \$3, \$4, \$5)
            ON CONFLICT (agent_id) DO UPDATE SET
                status = EXCLUDED.status,
                last_updated = EXCLUDED.last_updated,
                metadata = EXCLUDED.metadata,
                version = {self.table_name}.version + 1
            WHERE {self.table_name}.version = \$6
            RETURNING version
        """
        
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval(
                    query,
                    state.agent_id,
                    state.status,
                    state.last_updated,
                    json.dumps(state.metadata),
                    state.version + 1,
                    state.version
                )
                
                if result is None:
                    raise VersionConflictError(f"Version conflict for agent {state.agent_id}")
                
                state.version = result
                return state
                
            except asyncpg.PostgresError as e:
                self.logger.error(f"PostgreSQL error: {str(e)}")
                raise StateManagerError("Database operation failed") from e
    
    async def load_state(self, agent_id: str) -> Optional[AgentState]:
        if not self.pool:
            raise ConnectionError("Connection pool not initialized")
            
        query = f"""
            SELECT status, last_updated, metadata, version
            FROM {self.table_name}
            WHERE agent_id = \$1
        """
        
        async with self.pool.acquire() as conn:
            record = await conn.fetchrow(query, agent_id)
            if not record:
                return None
                
            return AgentState(
                agent_id=agent_id,
                status=record['status'],
                last_updated=record['last_updated'],
                metadata=dict(record['metadata']),
                version=record['version']
            )
    
    async def delete_state(self, agent_id: str) -> None:
        if not self.pool:
            raise ConnectionError("Connection pool not initialized")
            
        query = f"""
            DELETE FROM {self.table_name}
            WHERE agent_id = \$1
        """
        
        async with self.pool.acquire() as conn:
            result = await conn.execute(query, agent_id)
            if result == "DELETE 0":
                raise NotFoundError(f"State not found for agent {agent_id}")
    
    async def exists(self, agent_id: str) -> bool:
        if not self.pool:
            raise ConnectionError("Connection pool not initialized")
            
        query = f"""
            SELECT EXISTS(
                SELECT 1 FROM {self.table_name}
                WHERE agent_id = \$1
            )
        """
        
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, agent_id)
    
    async def health_check(self) -> bool:
        try:
            if not self.pool:
                return False
            async with self.pool.acquire() as conn:
                return await conn.fetchval("SELECT 1") == 1
        except asyncpg.PostgresError:
            return False

# Example Unit Tests (pytest style)
if __name__ == "__main__":
    import pytest
    import asyncio
    
    @pytest.fixture
    def redis_manager():
        manager = RedisStateManager()
        manager.save_state(AgentState(
            agent_id="test1",
            status="RUNNING",
            last_updated=datetime.now(),
            metadata={"cpu_usage": 0.4},
            version=0
        ))
        yield manager
        manager.delete_state("test1")
    
    @pytest.fixture
    async def pg_manager():
        manager = PostgresStateManager(dsn="postgresql://postgres@localhost/test")
        await manager.initialize()
        yield manager
        await manager.delete_state("test1")
    
    def test_redis_basic_operations(redis_manager):
        state = redis_manager.load_state("test1")
        assert state.status == "RUNNING"
    
    async def test_pg_basic_operations(pg_manager):
        test_state = AgentState(
            agent_id="test1",
            status="RUNNING",
            last_updated=datetime.now(),
            metadata={"cpu_usage": 0.4},
            version=0
        )
        saved_state = await pg_manager.save_state(test_state)
        assert saved_state.version == 1
