"""Jules Agent SDK - A user-friendly Python SDK for the Jules API."""

from jules_agent_sdk.client import JulesClient
from jules_agent_sdk.async_client import AsyncJulesClient
from jules_agent_sdk.exceptions import (
    JulesAPIError,
    JulesAuthenticationError,
    JulesNotFoundError,
    JulesValidationError,
    JulesRateLimitError,
)
from jules_agent_sdk.orchestrator import (
    JulesOrchestrator,
    OrchestratorConfig,
    TaskConfig,
    TaskResult,
    OrchestratorError,
    TaskFailed,
    DAGCycleError,
    ErrorPolicy,
)

__version__ = "0.1.0"
__all__ = [
    "JulesClient",
    "AsyncJulesClient",
    "JulesAPIError",
    "JulesAuthenticationError",
    "JulesNotFoundError",
    "JulesValidationError",
    "JulesRateLimitError",
    "JulesOrchestrator",
    "OrchestratorConfig",
    "TaskConfig",
    "TaskResult",
    "OrchestratorError",
    "TaskFailed",
    "DAGCycleError",
    "ErrorPolicy",
]
