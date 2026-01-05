"""
Converters for content manipulation processors.

This module provides converters for processors that work with FlowFile content
like HashContent, ReplaceText, etc.
"""

import textwrap
from typing import Dict, List

from nifi2py.models import Processor, ConversionResult
from nifi2py.converters.base import ProcessorConverter, register_converter


@register_converter
class HashContentConverter(ProcessorConverter):
    """
    Converter for HashContent processor.

    HashContent computes a hash of the FlowFile content and stores
    it in an attribute.
    """

    processor_types = ["org.apache.nifi.processors.standard.HashContent"]

    def convert(self, processor: Processor) -> ConversionResult:
        """
        Convert HashContent processor to Python code.

        Properties used:
        - Hash Attribute Name: Name of attribute to store hash (default: hash.value)
        - Hash Algorithm: Algorithm to use (MD5, SHA-1, SHA-256, etc.)

        Args:
            processor: HashContent processor to convert

        Returns:
            ConversionResult with generated hash code
        """
        function_name = self.generate_function_name(processor)

        # Get processor properties
        hash_attr_name = processor.get_property('Hash Attribute Name', 'hash.value')
        hash_algorithm = processor.get_property('Hash Algorithm', 'MD5')

        # Map NiFi algorithm names to Python hashlib names
        algorithm_map = {
            'MD5': 'md5',
            'SHA-1': 'sha1',
            'SHA-256': 'sha256',
            'SHA-384': 'sha384',
            'SHA-512': 'sha512',
        }

        python_algorithm = algorithm_map.get(hash_algorithm, hash_algorithm.lower().replace('-', ''))

        # Build function code
        code = f'''import hashlib
from typing import Dict, List
from nifi2py.models import FlowFile


def {function_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
{self.generate_docstring(processor)}
    try:
        # Compute hash of content
        hash_obj = hashlib.{python_algorithm}(flowfile.content)
        hash_value = hash_obj.hexdigest()

        # Store hash in attribute
        flowfile.attributes['{hash_attr_name}'] = hash_value

        # Return flowfile on success relationship
        return {{"success": [flowfile]}}

    except Exception as e:
        # Hash computation failed
        # In NiFi this would route to failure relationship
        # For now, we'll raise the exception
        raise RuntimeError(f"Hash computation failed: {{e}}")'''

        return ConversionResult(
            processor_id=processor.id,
            processor_name=processor.name,
            processor_type=processor.type,
            function_name=function_name,
            function_code=code,
            is_stub=False,
            dependencies=['hashlib', 'typing', 'nifi2py.models'],
            notes=f"Successfully converted HashContent processor using {hash_algorithm}",
            coverage_percentage=100
        )


@register_converter
class ReplaceTextConverter(ProcessorConverter):
    """
    Converter for ReplaceText processor.

    ReplaceText performs regex-based text replacement on FlowFile content.
    """

    processor_types = ["org.apache.nifi.processors.standard.ReplaceText"]

    def convert(self, processor: Processor) -> ConversionResult:
        """
        Convert ReplaceText processor to Python code.

        Properties used:
        - Search Value: Regex pattern to search for
        - Replacement Value: Replacement text (supports EL and backreferences)
        - Character Set: Character encoding (default: UTF-8)
        - Replacement Strategy: How to perform replacement

        Args:
            processor: ReplaceText processor to convert

        Returns:
            ConversionResult with generated replacement code
        """
        function_name = self.generate_function_name(processor)

        # Get processor properties
        search_value = processor.get_property('Search Value', '')
        replacement_value = processor.get_property('Replacement Value', '')
        character_set = processor.get_property('Character Set', 'UTF-8')
        replacement_strategy = processor.get_property('Replacement Strategy', 'Regex Replace')

        # Escape quotes in pattern and replacement
        search_escaped = search_value.replace('\\', '\\\\').replace("'", "\\'")
        replacement_escaped = replacement_value.replace('\\', '\\\\').replace("'", "\\'")

        # Build function code based on strategy
        if replacement_strategy == 'Regex Replace':
            replace_code = f"    new_content = re.sub(r'{search_escaped}', r'{replacement_escaped}', content_str)"
        elif replacement_strategy == 'Literal Replace':
            replace_code = f"    new_content = content_str.replace('{search_escaped}', '{replacement_escaped}')"
        else:
            # Prepend/Append/etc - use stub for complex strategies
            return self.create_stub_result(
                processor,
                notes=f"Replacement strategy '{replacement_strategy}' requires manual implementation",
                migration_hints=[
                    f"Review replacement strategy: {replacement_strategy}",
                    "Implement custom replacement logic"
                ]
            )

        code = f'''import re
from typing import Dict, List
from nifi2py.models import FlowFile


def {function_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
{self.generate_docstring(processor)}
    try:
        # Decode content
        content_str = flowfile.content.decode('{character_set}')

        # Perform replacement
{replace_code}

        # Encode back to bytes
        flowfile.content = new_content.encode('{character_set}')

        # Return flowfile on success relationship
        return {{"success": [flowfile]}}

    except Exception as e:
        # Replacement failed
        raise RuntimeError(f"Text replacement failed: {{e}}")'''

        warnings = []
        if '${' in replacement_value:
            warnings.append("Replacement value contains EL expressions - may need manual review")

        return ConversionResult(
            processor_id=processor.id,
            processor_name=processor.name,
            processor_type=processor.type,
            function_name=function_name,
            function_code=code,
            is_stub=False,
            dependencies=['re', 'typing', 'nifi2py.models'],
            notes="Successfully converted ReplaceText processor",
            coverage_percentage=90,
            warnings=warnings
        )
