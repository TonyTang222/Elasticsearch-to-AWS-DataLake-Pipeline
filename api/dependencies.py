"""FastAPI dependency injection functions."""

from concurrent.futures import ThreadPoolExecutor

from fastapi import Request

from api.run_store import RunStore


def get_run_store(request: Request) -> RunStore:
    return request.app.state.run_store


def get_executor(request: Request) -> ThreadPoolExecutor:
    return request.app.state.executor
