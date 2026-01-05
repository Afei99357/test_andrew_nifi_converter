"""
Base converter class for NiFi processor to Python function conversion.

This module provides the abstract base class that all processor converters
must inherit from, along with helper utilities for code generation.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Type
import re
import logging

from nifi2py.models import Processor, ConversionResult


logger = logging.getLogger(__name__)


# Global registry mapping processor types to converter instances
_CONVERTER_REGISTRY: Dict[str, 'ProcessorConverter'] = {}

# Fallback stub converter
_STUB_CONVERTER: Optional['ProcessorConverter'] = None


def register_converter(converter_class: Type['ProcessorConverter']) -> Type['ProcessorConverter']:
    """
    Decorator to register a processor converter.

    This decorator registers a converter class for all processor types
    it declares in its processor_types list.

    Args:
        converter_class: The converter class to register

    Returns:
        The converter class (allows use as decorator)

    Example:
        >>> @register_converter
        ... class UpdateAttributeConverter(ProcessorConverter):
        ...     processor_types = ["org.apache.nifi.processors.attributes.UpdateAttribute"]
        ...     ...
    """
    # Instantiate the converter
    converter = converter_class()

    # Register for all declared processor types
    for proc_type in converter.processor_types:
        if proc_type == "*":
            # This is the fallback stub converter
            global _STUB_CONVERTER
            _STUB_CONVERTER = converter
            logger.info(f"Registered stub converter: {converter_class.__name__}")
        else:
            _CONVERTER_REGISTRY[proc_type] = converter
            logger.debug(f"Registered converter for {proc_type}: {converter_class.__name__}")

    return converter_class


def get_converter(processor_type: str) -> Optional['ProcessorConverter']:
    """
    Get a converter for a specific processor type.

    Args:
        processor_type: Fully qualified NiFi processor class name

    Returns:
        Converter instance if found, None otherwise
    """
    return _CONVERTER_REGISTRY.get(processor_type)


def get_stub_converter() -> Optional['ProcessorConverter']:
    """
    Get the fallback stub converter.

    Returns:
        Stub converter instance if registered, None otherwise
    """
    return _STUB_CONVERTER


def get_registered_types() -> Dict[str, str]:
    """
    Get all registered processor types and their converter class names.

    Returns:
        Dictionary mapping processor types to converter class names
    """
    result = {}
    for proc_type, converter in _CONVERTER_REGISTRY.items():
        result[proc_type] = converter.__class__.__name__
    return result


class ProcessorConverter(ABC):
    """
    Abstract base class for all processor converters.

    Each converter translates a specific NiFi processor type (or set of types)
    into equivalent Python code. Subclasses must implement the convert() method
    and specify which processor types they support.

    Example:
        >>> @register_converter
        ... class UpdateAttributeConverter(ProcessorConverter):
        ...     processor_types = ["org.apache.nifi.processors.attributes.UpdateAttribute"]
        ...
        ...     def convert(self, processor: Processor) -> ConversionResult:
        ...         # Generate Python code
        ...         ...
    """

    # List of fully qualified NiFi processor class names this converter handles
    processor_types: List[str] = []

    @abstractmethod
    def convert(self, processor: Processor) -> ConversionResult:
        """
        Convert a NiFi processor configuration to Python code.

        Args:
            processor: The NiFi processor to convert

        Returns:
            ConversionResult containing generated Python code and metadata
        """
        pass

    def generate_function_name(self, processor: Processor) -> str:
        """
        Generate a valid Python function name from a processor.

        Creates a name like: process_<simple_type>_<short_id>
        Example: process_update_attribute_abc123

        Args:
            processor: The processor to generate a name for

        Returns:
            Valid Python function name
        """
        # Get simple type name (e.g., "UpdateAttribute" from full class name)
        simple_type = processor.processor_simple_type

        # Convert to snake_case
        snake_case = self._to_snake_case(simple_type)

        # Get short ID (first 6 chars)
        short_id = processor.id.replace("-", "")[:6]

        return f"process_{snake_case}_{short_id}"

    def _to_snake_case(self, name: str) -> str:
        """
        Convert CamelCase to snake_case.

        Args:
            name: CamelCase string

        Returns:
            snake_case string
        """
        # Insert underscore before uppercase letters (except first)
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        # Insert underscore before uppercase letters followed by lowercase
        s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        return s2.lower()

    def generate_docstring(self, processor: Processor, description: Optional[str] = None) -> str:
        """
        Generate a docstring for the converted function.

        Args:
            processor: The processor being converted
            description: Optional custom description

        Returns:
            Formatted docstring
        """
        desc = description or f"Generated from NiFi {processor.processor_simple_type} processor"

        lines = [
            f'    """',
            f'    {desc}',
            f'    ',
            f'    Original NiFi Processor:',
            f'      - ID: {processor.id}',
            f'      - Name: {processor.name}',
            f'      - Type: {processor.type}',
        ]

        if processor.properties:
            lines.append(f'      - Properties: {len(processor.properties)}')

        if processor.relationships:
            rel_names = [r.name for r in processor.relationships]
            lines.append(f'      - Relationships: {", ".join(rel_names)}')

        lines.extend([
            f'    ',
            f'    Args:',
            f'        flowfile: Input FlowFile to process',
            f'    ',
            f'    Returns:',
            f'        Dictionary mapping relationship names to lists of FlowFiles',
            f'    """',
        ])

        return '\n'.join(lines)

    def create_stub_result(
        self,
        processor: Processor,
        notes: Optional[str] = None,
        migration_hints: Optional[List[str]] = None
    ) -> ConversionResult:
        """
        Create a stub conversion result for unsupported processors.

        Args:
            processor: The processor being converted
            notes: Optional notes about why this is a stub
            migration_hints: Optional migration hints for manual implementation

        Returns:
            ConversionResult marked as a stub
        """
        function_name = self.generate_function_name(processor)

        # Build stub code
        code_lines = [
            'from typing import Dict, List',
            'from nifi2py.models import FlowFile',
            '',
            '',
            f'def {function_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:',
            self.generate_docstring(processor, "STUB: Manual implementation required"),
            '    # TODO: Manual implementation required',
            f'    # Processor Type: {processor.type}',
        ]

        # Add properties as comments
        if processor.properties:
            code_lines.append('    # Properties:')
            for key, value in processor.properties.items():
                if value:
                    code_lines.append(f'    #   {key}: {value}')
                else:
                    code_lines.append(f'    #   {key}: (not set)')

        # Add migration hints
        if migration_hints:
            code_lines.append('    #')
            code_lines.append('    # MIGRATION HINTS:')
            for hint in migration_hints:
                code_lines.append(f'    # - {hint}')

        code_lines.extend([
            '    #',
            '    raise NotImplementedError(',
            f'        "Converter for {processor.processor_simple_type} not yet implemented"',
            '    )',
        ])

        function_code = '\n'.join(code_lines)

        return ConversionResult(
            processor_id=processor.id,
            processor_name=processor.name,
            processor_type=processor.type,
            function_name=function_name,
            function_code=function_code,
            is_stub=True,
            dependencies=['typing', 'nifi2py.models'],
            notes=notes or "Manual implementation required",
            coverage_percentage=0
        )
