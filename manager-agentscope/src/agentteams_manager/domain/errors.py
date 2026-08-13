"""Domain-level failures without transport-specific details.

定义不依赖 Matrix、HTTP 或 CLI 的业务错误类型。

例如“资源不存在”和“状态转换不允许”是领域事实，不应在 workflow 内被写死成某个
HTTP 状态码。传输层可以把同一个错误分别翻译成 Matrix 回复、控制台响应或日志，
同时保留调用方可判断的稳定错误类别。
"""


class ManagerError(Exception):
    """Base class for expected Manager failures."""


class ConflictError(ManagerError):
    """The requested transition conflicts with current durable state."""


class NotFoundError(ManagerError):
    """A required resource does not exist."""


class PermissionDeniedError(ManagerError):
    """Room or resource policy denied an operation."""


class AmbiguousEffectError(ManagerError):
    """An external effect may have happened but could not be confirmed."""


class InvalidTransitionError(ManagerError):
    """A domain state transition is not permitted."""


class RecoveryError(ManagerError):
    """Durable recovery evidence is invalid or incomplete."""
