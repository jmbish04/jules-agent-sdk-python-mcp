"""Parallel task orchestrator for DAG-based workflow execution.

This module provides a robust, parallel task orchestrator designed for serverless
environments like Cloudflare Workers (via Pyodide). It uses modern async primitives
and structured concurrency for safe, predictable execution.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional
import os
import time

import anyio
from anyio import CapacityLimiter, create_task_group, fail_after
from pydantic import BaseModel, Field, field_validator
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
import structlog

logger = structlog.get_logger(__name__)


def _get_cpu_count_safe() -> int:
    """Get CPU count with fallback for restricted environments like Cloudflare Workers."""
    try:
        count = os.cpu_count()
        return max(4, count if count is not None else 4)
    except Exception:
        return 4


class OrchestratorError(Exception):
    """Base exception for orchestrator errors."""

    pass


class TaskFailed(OrchestratorError):
    """Exception raised when a task fails execution."""

    def __init__(self, task_id: str, exc: BaseException):
        super().__init__(f"Task {task_id} failed: {exc}")
        self.task_id = task_id
        self.exc = exc


class DAGCycleError(OrchestratorError):
    """Exception raised when a cycle is detected in the task graph."""

    pass


class ErrorPolicy(str):
    """Error handling policy for the orchestrator."""

    FAIL_FAST = "fail_fast"
    CONTINUE = "continue"


class TaskConfig(BaseModel):
    """Configuration for a single task in the DAG."""

    model_config = {"arbitrary_types_allowed": True}

    id: str
    fn: Callable[..., Awaitable[Any]]
    deps: List[str] = Field(default_factory=list)
    timeout_s: Optional[float] = None
    retry_attempts: int = 0
    retry_backoff_base: float = 0.5
    retry_backoff_max: float = 8.0
    concurrency_label: Optional[str] = None

    @field_validator("id")
    @classmethod
    def _non_empty(cls, v: str) -> str:
        if not v or v.strip() == "":
            raise ValueError("id must be non-empty")
        return v


class OrchestratorConfig(BaseModel):
    """Configuration for the JulesOrchestrator."""

    model_config = {"arbitrary_types_allowed": True}

    error_policy: str = ErrorPolicy.FAIL_FAST
    # Worker-safe default for CPU count
    max_concurrency: int = Field(default_factory=_get_cpu_count_safe)
    per_label_limits: Dict[str, int] = Field(default_factory=dict)
    capture_partial_results: bool = True
    # Hooks for persistence and observability
    on_persist: Optional[Callable[[str, Any], Awaitable[None]]] = None
    on_event: Optional[Callable[[str, str, Any], Awaitable[None]]] = None


@dataclass
class TaskResult:
    """Result of a task execution."""

    task_id: str
    started_at: float
    ended_at: float
    success: bool
    result: Any = None
    error: Optional[str] = None


class JulesOrchestrator:
    """A DAG-based parallel task orchestrator.

    This orchestrator executes tasks in parallel while respecting dependencies,
    concurrency limits, and error policies. It's designed to work in serverless
    environments like Cloudflare Workers.

    Example:
        async def task_a(ctx):
            return "result_a"

        async def task_b(ctx):
            return f"result_b based on {ctx['task_a']}"

        config = OrchestratorConfig()
        orchestrator = JulesOrchestrator(config)

        tasks = [
            TaskConfig(id="task_a", fn=task_a),
            TaskConfig(id="task_b", fn=task_b, deps=["task_a"]),
        ]

        results = await orchestrator.run(tasks)
    """

    def __init__(self, cfg: OrchestratorConfig):
        """Initialize the orchestrator.

        Args:
            cfg: Configuration for the orchestrator.
        """
        self.cfg = cfg
        self._global_limit = CapacityLimiter(cfg.max_concurrency)
        self._label_limits: Dict[str, CapacityLimiter] = {
            label: CapacityLimiter(limit) for label, limit in cfg.per_label_limits.items()
        }

    def _get_limiter(self, label: Optional[str]) -> CapacityLimiter:
        """Get the appropriate capacity limiter for a task.

        Args:
            label: Optional concurrency label for the task.

        Returns:
            The capacity limiter to use.
        """
        if label and label in self._label_limits:
            return self._label_limits[label]
        return self._global_limit

    async def _run_task(self, t: TaskConfig, ctx: Dict[str, Any]) -> Any:
        """Execute a single task with retry and timeout handling.

        Args:
            t: Task configuration.
            ctx: Context dictionary containing results from completed tasks.

        Returns:
            The result of the task execution.
        """
        retryer = None
        if t.retry_attempts > 0:
            retryer = AsyncRetrying(
                stop=stop_after_attempt(t.retry_attempts),
                wait=wait_exponential_jitter(initial=t.retry_backoff_base, max=t.retry_backoff_max),
                reraise=True,
            )

        async def invoke() -> Any:
            if t.timeout_s is not None and t.timeout_s > 0:
                with fail_after(t.timeout_s):
                    return await t.fn(ctx)
            return await t.fn(ctx)

        limiter = self._get_limiter(t.concurrency_label)
        async with limiter:
            if retryer:
                async for attempt in retryer:
                    with attempt:
                        return await invoke()
            else:
                return await invoke()

    def _validate_dag(self, tasks: Dict[str, TaskConfig]) -> None:
        """Validate that the task graph is a valid DAG (no cycles).

        Args:
            tasks: Dictionary of task configurations keyed by task ID.

        Raises:
            OrchestratorError: If a task depends on an unknown task.
            DAGCycleError: If a cycle is detected in the task graph.
        """
        indeg: Dict[str, int] = {tid: 0 for tid in tasks}
        adj: Dict[str, List[str]] = defaultdict(list)
        for tid, t in tasks.items():
            for d in t.deps:
                if d not in tasks:
                    raise OrchestratorError(f"Task {tid} depends on unknown task {d}")
                indeg[tid] += 1
                adj[d].append(tid)

        q = deque([tid for tid, deg in indeg.items() if deg == 0])
        visited = 0
        while q:
            u = q.popleft()
            visited += 1
            for v in adj[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
        if visited != len(tasks):
            raise DAGCycleError("Cycle detected in task graph")

    async def run(
        self, tasks: Iterable[TaskConfig], initial_ctx: Optional[Dict[str, Any]] = None
    ) -> Dict[str, TaskResult]:
        """Execute all tasks in the DAG respecting dependencies.

        Args:
            tasks: Iterable of task configurations.
            initial_ctx: Optional initial context dictionary.

        Returns:
            Dictionary mapping task IDs to their results.

        Raises:
            TaskFailed: If a task fails and error_policy is FAIL_FAST.
            DAGCycleError: If a cycle is detected in the task graph.
            OrchestratorError: If a task depends on an unknown task.
        """
        task_map: Dict[str, TaskConfig] = {t.id: t for t in tasks}
        self._validate_dag(task_map)

        # Build indegree and adjacency
        indeg: Dict[str, int] = {tid: 0 for tid in task_map}
        children: Dict[str, List[str]] = defaultdict(list)
        for tid, t in task_map.items():
            for d in t.deps:
                indeg[tid] += 1
                children[d].append(tid)

        ctx: Dict[str, Any] = dict(initial_ctx or {})
        results: Dict[str, TaskResult] = {}

        # State tracking
        ready = deque([tid for tid, deg in indeg.items() if deg == 0])
        in_flight_count = 0
        state_change = anyio.Event()  # Signal to wake up the scheduler

        async with create_task_group() as tg:

            async def launch(tid: str) -> None:
                nonlocal in_flight_count
                tcfg = task_map[tid]
                start_ts = time.time()
                logger.info("task.start", task_id=tid)

                if self.cfg.on_event:
                    await self.cfg.on_event("task_start", tid, {"ts": start_ts})

                try:
                    res = await self._run_task(tcfg, ctx)
                    ctx[tid] = res
                    results[tid] = TaskResult(tid, start_ts, time.time(), True, result=res)
                    logger.info("task.success", task_id=tid)

                    if self.cfg.on_persist:
                        await self.cfg.on_persist(tid, res)

                except Exception as e:
                    results[tid] = TaskResult(tid, start_ts, time.time(), False, error=str(e))
                    logger.error("task.error", task_id=tid, error=str(e))
                    if self.cfg.error_policy == ErrorPolicy.FAIL_FAST:
                        raise TaskFailed(tid, e)
                finally:
                    # Notify dependents
                    newly_ready = False
                    for child in children.get(tid, []):
                        indeg[child] -= 1
                        if indeg[child] == 0:
                            ready.append(child)
                            newly_ready = True

                    in_flight_count -= 1
                    # Wake up the scheduler loop if we added tasks or if we are the last one out
                    if newly_ready or in_flight_count == 0:
                        state_change.set()

            # Pump loop - uses state_change.wait() instead of sleep to prevent premature exit
            while ready or in_flight_count > 0:
                # If we have nothing ready to launch, wait for a task to finish
                if not ready:
                    await state_change.wait()
                    state_change = anyio.Event()  # Reset event

                # Launch as many as possible
                while ready:
                    tid = ready.popleft()
                    in_flight_count += 1
                    tg.start_soon(launch, tid)

                # Yield to allow other coroutines to process if needed
                await anyio.sleep(0)

        return results
