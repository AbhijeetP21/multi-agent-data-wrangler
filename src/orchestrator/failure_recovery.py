"""Error handling and recovery mechanisms."""

import logging
from typing import Optional, Callable, Any, TypeVar, Generic
from enum import Enum
from functools import wraps

from src.common.types.pipeline import PipelineState, PipelineStep


logger = logging.getLogger(__name__)


class FailureStrategy(str, Enum):
    """Strategy for handling failures."""
    SKIP = "skip"  # Skip the failing step and continue
    RETRY = "retry"  # Retry the failing step
    ABORT = "abort"  # Abort the pipeline
    FALLBACK = "fallback"  # Use a fallback value


T = TypeVar('T')


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        backoff_factor: float = 2.0,
        max_delay: float = 60.0,
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay


class RecoveryAction:
    """Represents a recovery action to take."""
    
    def __init__(
        self,
        strategy: FailureStrategy,
        step: PipelineStep,
        error: Exception,
        recovery_data: Optional[Any] = None,
    ):
        self.strategy = strategy
        self.step = step
        self.error = error
        self.recovery_data = recovery_data


class FailureRecovery:
    """Handles error recovery for the pipeline."""
    
    def __init__(
        self,
        strategy: FailureStrategy = FailureStrategy.SKIP,
        retry_config: Optional[RetryConfig] = None,
    ):
        """Initialize failure recovery.
        
        Args:
            strategy: Default failure strategy
            retry_config: Configuration for retry behavior
        """
        self.strategy = strategy
        self.retry_config = retry_config or RetryConfig()
        self._recovery_history: list[RecoveryAction] = []
    
    def handle_failure(
        self,
        step: PipelineStep,
        error: Exception,
        state: PipelineState,
    ) -> tuple[FailureStrategy, Optional[PipelineState]]:
        """Handle a failure in the pipeline.
        
        Args:
            step: The pipeline step that failed
            error: The exception that occurred
            state: Current pipeline state
            
        Returns:
            Tuple of (strategy to use, updated state or None)
        """
        logger.error(f"Failure in step {step.value}: {str(error)}")
        
        # Record the failure
        action = RecoveryAction(
            strategy=self.strategy,
            step=step,
            error=error,
        )
        self._recovery_history.append(action)
        
        # Update state with error
        state.error = str(error)
        
        # Determine strategy based on configuration
        if self.strategy == FailureStrategy.ABORT:
            logger.error("Aborting pipeline due to failure")
            return FailureStrategy.ABORT, state
            
        elif self.strategy == FailureStrategy.SKIP:
            logger.warning(f"Skipping step {step.value} due to failure")
            # Move to next step
            next_step = self._get_next_step(step)
            if next_step:
                state.current_step = next_step
            return FailureStrategy.SKIP, state
            
        elif self.strategy == FailureStrategy.RETRY:
            logger.info(f"Retrying step {step.value}")
            return FailureStrategy.RETRY, None  # Signal to retry
            
        elif self.strategy == FailureStrategy.FALLBACK:
            logger.warning(f"Using fallback for step {step.value}")
            return FailureStrategy.FALLBACK, state
            
        return FailureStrategy.ABORT, state
    
    def _get_next_step(self, current: PipelineStep) -> Optional[PipelineStep]:
        """Get the next step in the pipeline."""
        steps = list(PipelineStep)
        try:
            current_idx = steps.index(current)
            if current_idx + 1 < len(steps):
                return steps[current_idx + 1]
        except ValueError:
            pass
        return None
    
    def get_recovery_history(self) -> list[RecoveryAction]:
        """Get the history of recovery actions."""
        return self._recovery_history.copy()
    
    def clear_history(self) -> None:
        """Clear the recovery history."""
        self._recovery_history.clear()


def with_retry(
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """Decorator to add retry logic to a function.
    
    Args:
        max_retries: Maximum number of retries
        delay: Initial delay between retries in seconds
        backoff: Backoff factor for delay
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}. "
                            f"Retrying in {current_delay}s..."
                        )
                        import time
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"All {max_retries + 1} attempts failed"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


def with_fallback(
    fallback_value: Any = None,
    exception: type = Exception,
):
    """Decorator to add fallback logic to a function.
    
    Args:
        fallback_value: Value to return on exception
        exception: Exception type to catch
        
    Returns:
        Decorated function
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            try:
                return func(*args, **kwargs)
            except exception as e:
                logger.warning(
                    f"Function {func.__name__} failed: {str(e)}. "
                    f"Using fallback value."
                )
                return fallback_value
        
        return wrapper
    return decorator


class CircuitBreaker:
    """Circuit breaker pattern implementation for fault tolerance."""
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.is_open = False
    
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute a function through the circuit breaker."""
        import time
        
        # Check if circuit is open
        if self.is_open:
            if self.last_failure_time:
                elapsed = time.time() - self.last_failure_time
                if elapsed >= self.recovery_timeout:
                    # Try to close the circuit
                    logger.info("Circuit breaker: attempting to close")
                    self.is_open = False
                    self.failure_count = 0
                else:
                    raise RuntimeError("Circuit breaker is open")
        
        try:
            result = func(*args, **kwargs)
            # Success - reset failure count
            self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                logger.error(f"Circuit breaker opened after {self.failure_count} failures")
                self.is_open = True
            
            raise
