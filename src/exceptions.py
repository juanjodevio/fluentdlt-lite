"""Custom exceptions for fldt_lite.

This module defines the exception hierarchy for the fldt_lite package.
All exceptions inherit from FluentDLTLiteError for easy catching of
package-specific errors.
"""


class FluentDLTLiteError(Exception):
    """Base exception for all fldt_lite errors.

    All custom exceptions in the fldt_lite package inherit from this class,
    allowing users to catch all package-specific errors with a single
    except clause.
    """

    pass


class ValidationError(FluentDLTLiteError):
    """Raised when input validation fails.

    This exception is raised during pipeline configuration when:
    - Invalid source or destination types are provided
    - Required parameters are missing
    - Parameter values are out of acceptable ranges
    - Type constraints are violated
    """

    pass


class ConfigurationError(FluentDLTLiteError):
    """Raised when pipeline configuration is invalid or incomplete.

    This exception is raised when:
    - Required configuration is missing (e.g., no destination set)
    - Configuration parameters are incompatible
    - Pipeline cannot be constructed from given configuration
    """

    pass


class ExecutionError(FluentDLTLiteError):
    """Raised when pipeline execution fails.

    This exception is raised during pipeline runtime when:
    - Source extraction fails
    - Transformation fails
    - Destination loading fails
    - Unexpected runtime errors occur

    The original exception is typically chained via __cause__.
    """

    pass
