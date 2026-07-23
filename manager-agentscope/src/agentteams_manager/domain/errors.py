"""Domain-level failures without transport-specific details."""


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

