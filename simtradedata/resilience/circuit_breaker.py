"""Three-state circuit breaker for protecting data source calls."""

import enum
import logging
import threading
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class CircuitState(enum.Enum):
    """Possible states of a circuit breaker."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker thresholds and timing.

    Attributes:
        failure_threshold: Number of consecutive failures before opening.
        success_threshold: Successes needed in HALF_OPEN to close again.
        timeout: Seconds to wait in OPEN before transitioning to HALF_OPEN.
    """

    failure_threshold: int = 5
    success_threshold: int = 3
    timeout: float = 60.0


class CircuitBreaker:
    """Thread-safe three-state circuit breaker.

    States:
        CLOSED  - Normal operation; failures are counted.
        OPEN    - Calls are blocked; waits for timeout to elapse.
        HALF_OPEN - Trial period; successes move to CLOSED, a failure
                    reopens immediately.
    """

    def __init__(
        self,
        name: str,
        config: CircuitBreakerConfig | None = None,
    ) -> None:
        self._name = name
        self._config = config or CircuitBreakerConfig()
        self._lock = threading.RLock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float = 0.0

    @property
    def state(self) -> CircuitState:
        """Return the current circuit state.

        If the circuit is OPEN and the timeout has elapsed, it
        automatically transitions to HALF_OPEN before returning.
        """
        with self._lock:
            if self._state is CircuitState.OPEN:
                elapsed = time.monotonic() - self._opened_at
                if elapsed >= self._config.timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
                    logger.info(
                        "Circuit '%s' transitioned to HALF_OPEN "
                        "after %.1fs timeout",
                        self._name,
                        elapsed,
                    )
            return self._state

    def is_available(self) -> bool:
        """Return True if the circuit is not OPEN (calls may proceed)."""
        return self.state is not CircuitState.OPEN

    def record_success(self) -> None:
        """Record a successful call.

        In HALF_OPEN: increments success count; if the success threshold
        is reached the circuit transitions to CLOSED.
        In CLOSED: resets the failure count.
        """
        with self._lock:
            current = self.state

            if current is CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info(
                        "Circuit '%s' transitioned to CLOSED "
                        "after %d consecutive successes",
                        self._name,
                        self._config.success_threshold,
                    )

            elif current is CircuitState.CLOSED:
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call.

        In HALF_OPEN: immediately transitions back to OPEN.
        In CLOSED: increments failure count; if the failure threshold
        is reached the circuit transitions to OPEN.
        """
        with self._lock:
            current = self.state

            if current is CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                self._success_count = 0
                logger.warning(
                    "Circuit '%s' transitioned to OPEN "
                    "from HALF_OPEN after failure",
                    self._name,
                )

            elif current is CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self._config.failure_threshold:
                    self._state = CircuitState.OPEN
                    self._opened_at = time.monotonic()
                    self._success_count = 0
                    logger.warning(
                        "Circuit '%s' transitioned to OPEN "
                        "after %d consecutive failures",
                        self._name,
                        self._failure_count,
                    )
