# postgresql.py
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, Optional

import asyncpg
from asyncpg import Connection, Pool, PostgresError
from asyncpg.transaction import Transaction

logger = logging.getLogger(__name__)

@dataclass
class PostgresConfig:
    host: str
    port: int = 5432
    user: str
    password: str
    database: str
    min_connections: int = 5
    max_connections: int = 20
    command_timeout: int = 30
    ssl: bool = True

class PostgresManager:
    def __init__(self, config: PostgresConfig):
        self.config = config
        self.pool: Optional[Pool] = None
        self._type_codecs_registered = False

    async def initialize(self) -> None:
        """Initialize connection pool with type codecs and connection validation"""
        if not self.pool or self.pool._closed:
            self.pool = await asyncpg.create_pool(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                min_size=self.config.min_connections,
                max_size=self.config.max_connections,
                command_timeout=self.config.command_timeout,
                ssl="require" if self.config.ssl else None,
                setup=self._setup_connection
            )
            logger.info("PostgreSQL connection pool initialized")

    async def _setup_connection(self, conn: Connection) -> None:
        """Per-connection setup including type codecs and session settings"""
        await self._register_type_codecs(conn)
        await conn.execute("SET TIME ZONE 'UTC';")

    async def _register_type_codecs(self, conn: Connection) -> None:
        """Register custom type codecs if not already registered"""
        if not self._type_codecs_registered:
            await conn.set_type_codec(
                'json',
                encoder=lambda v: json.dumps(v),
                decoder=lambda v: json.loads(v),
                schema='pg_catalog'
            )
            self._type_codecs_registered = True

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[Connection, None]:
        """Context manager for individual connection with health check"""
        if not self.pool:
            await self.initialize()
            
        async with self.pool.acquire() as conn:
            try:
                await conn.execute("SELECT 1")  # Connection health check
                yield conn
            except PostgresError as e:
                logger.error("Connection health check failed: %s", e)
                await self._reconnect()
                raise

    @asynccontextmanager
    async def transaction(self, savepoint_name: str = None) -> AsyncGenerator[Transaction, None]:
        """Transactional context manager with savepoint support"""
        async with self.connection() as conn:
            transaction = conn.transaction(savepoint_name)
            await transaction.start()
            try:
                yield transaction
                await transaction.commit()
            except Exception as e:
                await transaction.rollback()
                raise e

    async def execute(
        self,
        query: str,
        *args,
        retries: int = 3,
        backoff_factor: float = 0.5
    ) -> Any:
        """Execute query with retry logic and exponential backoff"""
        for attempt in range(retries + 1):
            try:
                async with self.connection() as conn:
                    start_time = time.monotonic()
                    result = await conn.execute(query, *args)
                    latency = (time.monotonic() - start_time) * 1000
                    logger.debug("Query executed in %.2fms: %s", latency, query[:200])
                    return result
            except (PostgresError, asyncio.TimeoutError) as e:
                if attempt == retries:
                    logger.error("Query failed after %d attempts: %s", retries, e)
                    raise
                delay = backoff_factor * (2 ** attempt)
                logger.warning("Retrying query in %.1fs (attempt %d/%d)", delay, attempt+1, retries)
                await asyncio.sleep(delay)

    async def fetch(
        self,
        query: str,
        *args,
        timeout: float = None,
        record_class: Any = None
    ) -> list:
        """Execute query and return results with optional record mapping"""
        async with self.connection() as conn:
            return await conn.fetch(query, *args, timeout=timeout, record_class=record_class)

    async def close(self) -> None:
        """Close connection pool gracefully"""
        if self.pool and not self.pool._closed:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")

    async def _reconnect(self) -> None:
        """Reconnect strategy for failed connections"""
        await self.close()
        await self.initialize()

    async def monitor_pool(self) -> Dict[str, Any]:
        """Return connection pool health metrics"""
        if self.pool:
            return {
                "connections": {
                    "min": self.pool._minsize,
                    "max": self.pool._maxsize,
                    "idle": len(self.pool._idle),
                    "used": self.pool._maxsize - self.pool._minsize - len(self.pool._idle)
                },
                "queue": {
                    "waiting": self.pool._queue.qsize(),
                    "wait_time_avg": self.pool._max_queries // self.pool._maxsize
                }
            }
        return {"status": "pool_not_initialized"}

class PostgresErrorTypes:
    class ConnectionTimeout(Exception): ...
    class QueryTimeout(Exception): ...
    class ConstraintViolation(Exception): ...
    class DeadlockDetected(Exception): ...

def map_postgres_error(error: PostgresError) -> Exception:
    """Map PostgreSQL error codes to specific exception types"""
    error_code = error.sqlstate
    error_map = {
        '40P01': PostgresErrorTypes.DeadlockDetected,
        '23505': PostgresErrorTypes.ConstraintViolation,
        '57014': PostgresErrorTypes.QueryTimeout,
        '08006': PostgresErrorTypes.ConnectionTimeout
    }
    return error_map.get(error_code, PostgresError)(str(error))

# Example Usage
async def main():
    config = PostgresConfig(
        host="prod-db.azeerc.ai",
        user="ai_service",
        password="secure_password",
        database="agent_system"
    )
    
    db = PostgresManager(config)
    await db.initialize()
    
    try:
        async with db.transaction():
            await db.execute(
                "INSERT INTO agent_events (type, data) VALUES (\$1, \$2)",
                "startup",
                {"environment": "production"}
            )
            results = await db.fetch("SELECT * FROM agent_configs WHERE active = true")
            print(f"Active configurations: {len(results)}")
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())
