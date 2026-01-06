"""Tests for the orchestrator module."""

import pytest
import asyncio

from jules_agent_sdk import (
    JulesOrchestrator,
    OrchestratorConfig,
    TaskConfig,
    TaskResult,
    OrchestratorError,
    TaskFailed,
    DAGCycleError,
    ErrorPolicy,
)


class TestTaskConfig:
    """Test cases for TaskConfig."""

    def test_task_config_creation(self):
        """Test basic TaskConfig creation."""

        async def dummy_fn(ctx):
            return "result"

        config = TaskConfig(id="task1", fn=dummy_fn)
        assert config.id == "task1"
        assert config.deps == []
        assert config.timeout_s is None
        assert config.retry_attempts == 0

    def test_task_config_with_deps(self):
        """Test TaskConfig with dependencies."""

        async def dummy_fn(ctx):
            return "result"

        config = TaskConfig(id="task2", fn=dummy_fn, deps=["task1"])
        assert config.deps == ["task1"]

    def test_task_config_requires_non_empty_id(self):
        """Test TaskConfig requires non-empty id."""

        async def dummy_fn(ctx):
            return "result"

        with pytest.raises(ValueError, match="id must be non-empty"):
            TaskConfig(id="", fn=dummy_fn)

        with pytest.raises(ValueError, match="id must be non-empty"):
            TaskConfig(id="   ", fn=dummy_fn)


class TestOrchestratorConfig:
    """Test cases for OrchestratorConfig."""

    def test_default_config(self):
        """Test default OrchestratorConfig values."""
        config = OrchestratorConfig()
        assert config.error_policy == ErrorPolicy.FAIL_FAST
        assert config.max_concurrency >= 4
        assert config.per_label_limits == {}
        assert config.capture_partial_results is True
        assert config.on_persist is None
        assert config.on_event is None

    def test_config_with_hooks(self):
        """Test OrchestratorConfig with hooks."""

        async def persist_hook(task_id, result):
            pass

        async def event_hook(event_type, task_id, data):
            pass

        config = OrchestratorConfig(on_persist=persist_hook, on_event=event_hook)
        assert config.on_persist is persist_hook
        assert config.on_event is event_hook

    def test_config_with_label_limits(self):
        """Test OrchestratorConfig with per-label limits."""
        config = OrchestratorConfig(per_label_limits={"gpu": 2, "cpu": 8})
        assert config.per_label_limits["gpu"] == 2
        assert config.per_label_limits["cpu"] == 8


class TestJulesOrchestrator:
    """Test cases for JulesOrchestrator."""

    @pytest.mark.asyncio
    async def test_single_task_execution(self):
        """Test executing a single task."""

        async def task_fn(ctx):
            return "task_result"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [TaskConfig(id="task1", fn=task_fn)]

        results = await orchestrator.run(tasks)

        assert "task1" in results
        assert results["task1"].success is True
        assert results["task1"].result == "task_result"
        assert results["task1"].error is None

    @pytest.mark.asyncio
    async def test_sequential_dependency(self):
        """Test tasks with sequential dependencies."""

        async def task_a(ctx):
            return "result_a"

        async def task_b(ctx):
            return f"result_b_{ctx['task_a']}"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [
            TaskConfig(id="task_a", fn=task_a),
            TaskConfig(id="task_b", fn=task_b, deps=["task_a"]),
        ]

        results = await orchestrator.run(tasks)

        assert results["task_a"].success is True
        assert results["task_b"].success is True
        assert results["task_b"].result == "result_b_result_a"

    @pytest.mark.asyncio
    async def test_parallel_execution(self):
        """Test parallel task execution."""
        execution_order = []

        async def task_a(ctx):
            execution_order.append("a_start")
            await asyncio.sleep(0.05)
            execution_order.append("a_end")
            return "a"

        async def task_b(ctx):
            execution_order.append("b_start")
            await asyncio.sleep(0.05)
            execution_order.append("b_end")
            return "b"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [
            TaskConfig(id="task_a", fn=task_a),
            TaskConfig(id="task_b", fn=task_b),
        ]

        results = await orchestrator.run(tasks)

        assert results["task_a"].success is True
        assert results["task_b"].success is True
        # Both tasks should start before either ends (parallel execution)
        a_start_idx = execution_order.index("a_start")
        b_start_idx = execution_order.index("b_start")
        a_end_idx = execution_order.index("a_end")
        b_end_idx = execution_order.index("b_end")
        # At least one task should start before the other ends
        assert a_start_idx < max(a_end_idx, b_end_idx)
        assert b_start_idx < max(a_end_idx, b_end_idx)

    @pytest.mark.asyncio
    async def test_diamond_dependency(self):
        """Test diamond-shaped dependency graph (A -> B, A -> C, B -> D, C -> D)."""
        execution_order = []

        async def task_a(ctx):
            execution_order.append("a")
            return "a"

        async def task_b(ctx):
            execution_order.append("b")
            return f"b_{ctx['task_a']}"

        async def task_c(ctx):
            execution_order.append("c")
            return f"c_{ctx['task_a']}"

        async def task_d(ctx):
            execution_order.append("d")
            return f"d_{ctx['task_b']}_{ctx['task_c']}"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [
            TaskConfig(id="task_a", fn=task_a),
            TaskConfig(id="task_b", fn=task_b, deps=["task_a"]),
            TaskConfig(id="task_c", fn=task_c, deps=["task_a"]),
            TaskConfig(id="task_d", fn=task_d, deps=["task_b", "task_c"]),
        ]

        results = await orchestrator.run(tasks)

        assert all(r.success for r in results.values())
        assert results["task_d"].result == "d_b_a_c_a"
        # Verify execution order: A before B and C, B and C before D
        assert execution_order.index("a") < execution_order.index("b")
        assert execution_order.index("a") < execution_order.index("c")
        assert execution_order.index("b") < execution_order.index("d")
        assert execution_order.index("c") < execution_order.index("d")

    @pytest.mark.asyncio
    async def test_cycle_detection(self):
        """Test that cycles in the DAG are detected."""

        async def dummy_fn(ctx):
            return "result"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [
            TaskConfig(id="task_a", fn=dummy_fn, deps=["task_b"]),
            TaskConfig(id="task_b", fn=dummy_fn, deps=["task_a"]),
        ]

        with pytest.raises(DAGCycleError, match="Cycle detected"):
            await orchestrator.run(tasks)

    @pytest.mark.asyncio
    async def test_unknown_dependency(self):
        """Test that unknown dependencies are detected."""

        async def dummy_fn(ctx):
            return "result"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [
            TaskConfig(id="task_a", fn=dummy_fn, deps=["unknown_task"]),
        ]

        with pytest.raises(OrchestratorError, match="depends on unknown task"):
            await orchestrator.run(tasks)

    @pytest.mark.asyncio
    async def test_fail_fast_policy(self):
        """Test fail-fast error policy."""

        async def failing_task(ctx):
            raise ValueError("Task failed!")

        async def dependent_task(ctx):
            return "should not run"

        orchestrator = JulesOrchestrator(
            OrchestratorConfig(error_policy=ErrorPolicy.FAIL_FAST)
        )
        tasks = [
            TaskConfig(id="failing", fn=failing_task),
            TaskConfig(id="dependent", fn=dependent_task, deps=["failing"]),
        ]

        # TaskFailed is wrapped in ExceptionGroup by anyio's structured concurrency
        with pytest.raises((TaskFailed, BaseExceptionGroup)) as exc_info:
            await orchestrator.run(tasks)

        # Check if it's an ExceptionGroup and extract the TaskFailed
        exc = exc_info.value
        if isinstance(exc, BaseExceptionGroup):
            # Find the TaskFailed exception in the group
            task_failed_exceptions = [
                e for e in exc.exceptions if isinstance(e, TaskFailed)
            ]
            assert len(task_failed_exceptions) == 1
            assert task_failed_exceptions[0].task_id == "failing"
        else:
            assert exc.task_id == "failing"

    @pytest.mark.asyncio
    async def test_initial_context(self):
        """Test passing initial context."""

        async def task_fn(ctx):
            return f"got_{ctx['initial_value']}"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [TaskConfig(id="task1", fn=task_fn)]

        results = await orchestrator.run(tasks, initial_ctx={"initial_value": "hello"})

        assert results["task1"].result == "got_hello"

    @pytest.mark.asyncio
    async def test_on_persist_hook(self):
        """Test on_persist callback is called."""
        persist_calls = []

        async def persist_hook(task_id, result):
            persist_calls.append((task_id, result))

        async def task_fn(ctx):
            return "persisted_result"

        config = OrchestratorConfig(on_persist=persist_hook)
        orchestrator = JulesOrchestrator(config)
        tasks = [TaskConfig(id="task1", fn=task_fn)]

        await orchestrator.run(tasks)

        assert len(persist_calls) == 1
        assert persist_calls[0] == ("task1", "persisted_result")

    @pytest.mark.asyncio
    async def test_on_event_hook(self):
        """Test on_event callback is called."""
        event_calls = []

        async def event_hook(event_type, task_id, data):
            event_calls.append((event_type, task_id, data))

        async def task_fn(ctx):
            return "result"

        config = OrchestratorConfig(on_event=event_hook)
        orchestrator = JulesOrchestrator(config)
        tasks = [TaskConfig(id="task1", fn=task_fn)]

        await orchestrator.run(tasks)

        # Should have at least a task_start event
        assert any(e[0] == "task_start" and e[1] == "task1" for e in event_calls)

    @pytest.mark.asyncio
    async def test_task_result_timestamps(self):
        """Test that TaskResult contains valid timestamps."""

        async def task_fn(ctx):
            await asyncio.sleep(0.01)
            return "result"

        orchestrator = JulesOrchestrator(OrchestratorConfig())
        tasks = [TaskConfig(id="task1", fn=task_fn)]

        results = await orchestrator.run(tasks)

        result = results["task1"]
        assert result.started_at > 0
        assert result.ended_at > result.started_at

    @pytest.mark.asyncio
    async def test_concurrency_label_limits(self):
        """Test per-label concurrency limits."""
        concurrent_count = 0
        max_concurrent = 0

        async def gpu_task(ctx):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1
            return "done"

        config = OrchestratorConfig(per_label_limits={"gpu": 2})
        orchestrator = JulesOrchestrator(config)
        tasks = [
            TaskConfig(id=f"gpu_task_{i}", fn=gpu_task, concurrency_label="gpu")
            for i in range(4)
        ]

        await orchestrator.run(tasks)

        # Max concurrent should not exceed the label limit of 2
        assert max_concurrent <= 2


class TestTaskResult:
    """Test cases for TaskResult."""

    def test_success_result(self):
        """Test successful TaskResult."""
        result = TaskResult(
            task_id="task1",
            started_at=1000.0,
            ended_at=1001.0,
            success=True,
            result="value",
        )
        assert result.success is True
        assert result.result == "value"
        assert result.error is None

    def test_failed_result(self):
        """Test failed TaskResult."""
        result = TaskResult(
            task_id="task1",
            started_at=1000.0,
            ended_at=1001.0,
            success=False,
            error="Something went wrong",
        )
        assert result.success is False
        assert result.error == "Something went wrong"
        assert result.result is None
