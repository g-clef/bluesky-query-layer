# src/service.py
import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Protocol

import ray
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)

from src.actor import DuckDBQueryActor
from src.exceptions import QueryError, QueryTimeoutError, S3Error
from src.mcp import handle_mcp_request


class ActorProtocol(Protocol):
    def query(self, sql: str) -> list[dict]: ...
    def ping(self) -> bool: ...


class ActorProxy:
    def __init__(self, ray_actor: Any) -> None:
        self._actor = ray_actor

    def query(self, sql: str) -> list[dict]:
        try:
            return ray.get(self._actor.query.remote(sql), timeout=300)
        except ray.exceptions.GetTimeoutError as e:
            raise QueryTimeoutError("Query exceeded 300s timeout") from e
        except ray.exceptions.RayTaskError as e:
            raise e.cause

    def ping(self) -> bool:
        try:
            return ray.get(self._actor.ping.remote(), timeout=5)
        except Exception as e:
            logger.error("Actor ping failed: %s: %s", type(e).__name__, e)
            return False


_actor: ActorProtocol | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _actor
    ray_address = os.environ.get("RAY_ADDRESS")
    if ray_address:
        app_dir = str(Path(__file__).parent.parent)
        env_vars = {
            k: os.environ[k]
            for k in ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "PARQUET_GLOB")
            if k in os.environ
        }
        ray.init(
            address=ray_address,
            ignore_reinit_error=True,
            runtime_env={
                "working_dir": app_dir,
                "pip": ["duckdb>=1.0.0", "pyarrow>=15.0.0"],
                "env_vars": env_vars,
            },
        )
        remote_actor = DuckDBQueryActor.options(
            name="duckdb_query_actor",
            namespace="bluesky-query",
            lifetime="detached",
            get_if_exists=True,
        ).remote()
        _actor = ActorProxy(remote_actor)
    yield
    if ray_address:
        ray.shutdown()


app = FastAPI(lifespan=lifespan)


def get_actor() -> ActorProtocol:
    if _actor is None:
        raise HTTPException(status_code=503, detail="Actor not initialized")
    return _actor


class QueryRequest(BaseModel):
    sql: str


@app.get("/health")
async def health(actor: ActorProtocol = Depends(get_actor)) -> dict:
    loop = asyncio.get_running_loop()
    try:
        reachable = await loop.run_in_executor(None, actor.ping)
    except asyncio.CancelledError:
        raise HTTPException(status_code=503, detail="Actor unreachable")
    if not reachable:
        raise HTTPException(status_code=503, detail="Actor unreachable")
    return {"status": "ok"}


@app.post("/query")
async def query_endpoint(
    request: QueryRequest, actor: ActorProtocol = Depends(get_actor)
) -> dict:
    loop = asyncio.get_running_loop()
    try:
        rows = await loop.run_in_executor(None, actor.query, request.sql)
        return {"rows": rows}
    except QueryTimeoutError as e:
        raise HTTPException(status_code=408, detail=str(e))
    except QueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except S3Error as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.post("/mcp")
async def mcp_endpoint(
    request: Request, actor: ActorProtocol = Depends(get_actor)
) -> dict:
    try:
        body = await request.json()
    except Exception:
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
    if not isinstance(body, dict):
        return {"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "Invalid Request"}}
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, handle_mcp_request, body, actor)