"""
Processor converter registry and exports.

This module provides the registry system for NiFi processor converters,
allowing dynamic registration and lookup of converters by processor type.
"""

from typing import Dict, List
import logging

from nifi2py.models import Processor, ConversionResult
from nifi2py.converters.base import (
    ProcessorConverter,
    register_converter,
    get_converter,
    get_stub_converter,
    get_registered_types,
)


logger = logging.getLogger(__name__)


def convert_processor(processor: Processor) -> ConversionResult:
    """
    Convert a NiFi processor to Python code using registered converters.

    This is the main entry point for processor conversion. It looks up
    the appropriate converter and generates Python code.

    Args:
        processor: The NiFi processor to convert

    Returns:
        ConversionResult containing generated code and metadata

    Example:
        >>> result = convert_processor(processor)
        >>> if result.is_stub:
        ...     print("Manual implementation required")
        >>> else:
        ...     print(result.function_code)
    """
    # Look up converter for this processor type
    converter = get_converter(processor.type)

    if converter:
        logger.info(f"Converting {processor.name} ({processor.processor_simple_type}) using {converter.__class__.__name__}")
        return converter.convert(processor)
    else:
        # No converter found, use stub
        logger.warning(f"No converter found for {processor.type}, generating stub")
        stub_converter = get_stub_converter()

        if stub_converter:
            return stub_converter.convert(processor)
        else:
            # No stub converter either - create a minimal stub result
            # This should never happen since StubConverter registers for "*"
            from nifi2py.models import ConversionResult
            function_name = f"process_unknown_{processor.id.replace('-', '')[:6]}"
            return ConversionResult(
                processor_id=processor.id,
                processor_name=processor.name,
                processor_type=processor.type,
                function_name=function_name,
                function_code=f"def {function_name}():\n    raise NotImplementedError('No converter registered')",
                is_stub=True,
                dependencies=[],
                notes="No converter registered for this processor type",
                coverage_percentage=0
            )


def get_converter_coverage(processors: List[Processor]) -> Dict[str, int]:
    """
    Calculate converter coverage for a list of processors.

    Args:
        processors: List of processors to analyze

    Returns:
        Dictionary with coverage statistics:
        - total: Total number of processors
        - converted: Number with registered converters
        - stubbed: Number requiring stubs
        - coverage_percentage: Percentage with converters
    """
    total = len(processors)
    converted = 0
    stubbed = 0

    for proc in processors:
        if get_converter(proc.type):
            converted += 1
        else:
            stubbed += 1

    return {
        "total": total,
        "converted": converted,
        "stubbed": stubbed,
        "coverage_percentage": (converted / total * 100) if total > 0 else 0
    }


# Export all public APIs
__all__ = [
    "ProcessorConverter",
    "register_converter",
    "get_converter",
    "get_stub_converter",
    "convert_processor",
    "get_registered_types",
    "get_converter_coverage",
]


# Import all converter modules to trigger registration
# This ensures all converters are registered when the package is imported
def _register_all_converters():
    """Import all converter modules to trigger @register_converter decorators."""
    try:
        from nifi2py.converters import stubs
        from nifi2py.converters import standard
        from nifi2py.converters import attributes
        from nifi2py.converters import content
        from nifi2py.converters import http
        logger.info("All converters registered successfully")
    except ImportError as e:
        logger.warning(f"Some converters could not be imported: {e}")


# Register converters on module import
_register_all_converters()
