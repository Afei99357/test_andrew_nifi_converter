"""
Converters for attribute manipulation processors.

This module provides converters for processors that work with FlowFile attributes
like UpdateAttribute and RouteOnAttribute.
"""

import textwrap
import re
from typing import Dict, List, Tuple

from nifi2py.models import Processor, ConversionResult
from nifi2py.converters.base import ProcessorConverter, register_converter


@register_converter
class UpdateAttributeConverter(ProcessorConverter):
    """
    Converter for UpdateAttribute processor.

    UpdateAttribute adds or modifies FlowFile attributes using
    static values or NiFi Expression Language.
    """

    processor_types = ["org.apache.nifi.processors.attributes.UpdateAttribute"]

    def convert(self, processor: Processor) -> ConversionResult:
        """
        Convert UpdateAttribute processor to Python code.

        Generates code that updates FlowFile attributes based on
        processor properties. Each property becomes an attribute.

        Args:
            processor: UpdateAttribute processor to convert

        Returns:
            ConversionResult with generated attribute update code
        """
        function_name = self.generate_function_name(processor)

        # Filter out special properties
        attribute_updates = {}
        for key, value in processor.properties.items():
            if key not in ['Delete Attributes Expression', 'Store State', 'Stateful Variables Initial Value']:
                if value:  # Only include properties with values
                    attribute_updates[key] = value

        # Generate attribute update code
        update_lines = []
        dependencies = {'typing', 'nifi2py.models'}

        for attr_name, attr_value in attribute_updates.items():
            python_expr = self._simple_el_to_python(attr_value)

            # Track dependencies based on expressions
            if 'datetime' in python_expr or 'now()' in attr_value:
                dependencies.add('datetime')
            if 'uuid' in python_expr or 'uuid()' in attr_value:
                dependencies.add('uuid')

            update_lines.append(f"    flowfile.attributes['{attr_name}'] = {python_expr}")

        # Build imports
        import_lines = ['from typing import Dict, List', 'from nifi2py.models import FlowFile']
        if 'datetime' in dependencies:
            import_lines.append('from datetime import datetime')
        if 'uuid' in dependencies:
            import_lines.append('import uuid')

        imports = '\n'.join(import_lines)

        # Build update code
        if update_lines:
            updates = '\n'.join(update_lines)
        else:
            updates = '    # No attribute updates configured'

        # Build function code
        code = f'''{imports}


def {function_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
{self.generate_docstring(processor)}
    # Get attributes for expression evaluation
    attributes = flowfile.attributes

    # Update attributes
{updates}

    # Return flowfile on success relationship
    return {{"success": [flowfile]}}'''

        notes = f"Converted {len(attribute_updates)} attribute update(s)"
        if len(attribute_updates) == 0:
            notes += " - Warning: No attributes configured to update"

        return ConversionResult(
            processor_id=processor.id,
            processor_name=processor.name,
            processor_type=processor.type,
            function_name=function_name,
            function_code=code,
            is_stub=False,
            dependencies=list(dependencies),
            notes=notes,
            coverage_percentage=90,
            warnings=["Complex EL expressions may need manual review"] if any('${' in v for v in attribute_updates.values()) else []
        )

    def _simple_el_to_python(self, expression: str) -> str:
        """
        Simple EL to Python converter (placeholder).

        This is a simplified converter. In production, use the full EL transpiler.

        Args:
            expression: NiFi EL expression

        Returns:
            Python expression string
        """
        if not expression:
            return "''"

        # If no EL expressions, return as literal
        if '${' not in expression:
            return repr(expression)

        # Detect common patterns
        import re

        # Handle now():format() pattern
        if 'now()' in expression and 'format(' in expression:
            # Extract format pattern
            format_match = re.search(r"now\(\):format\('([^']+)'\)", expression)
            if format_match:
                nifi_format = format_match.group(1)
                python_format = self._convert_date_format(nifi_format)
                return f"datetime.now().strftime('{python_format}')"

        # Handle uuid() pattern
        if expression == '${uuid()}':
            return "str(uuid.uuid4())"

        # Handle simple attribute reference
        attr_match = re.match(r'\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}$', expression)
        if attr_match:
            attr_name = attr_match.group(1)
            return f"attributes.get('{attr_name}', '')"

        # Handle embedded expressions in strings
        def replace_el(match):
            expr = match.group(1)

            # Handle now():format()
            if 'now()' in expr and 'format(' in expr:
                format_match = re.search(r"now\(\):format\('([^']+)'\)", expr)
                if format_match:
                    nifi_format = format_match.group(1)
                    python_format = self._convert_date_format(nifi_format)
                    return "{datetime.now().strftime('" + python_format + "')}"

            # Handle uuid()
            if expr == 'uuid()':
                return "{str(uuid.uuid4())}"

            # Handle simple attribute
            if ':' not in expr:
                return "{attributes.get('" + expr + "', '')}"

            # Handle attribute with functions (simplified)
            attr_name = expr.split(':')[0]
            return "{attributes.get('" + attr_name + "', '')}"

        result = re.sub(r'\$\{([^}]+)\}', replace_el, expression)
        return f'f"{result}"'

    def _convert_date_format(self, nifi_format: str) -> str:
        """
        Convert NiFi date format to Python strftime format.

        Args:
            nifi_format: NiFi/Java date format

        Returns:
            Python strftime format
        """
        # Common mappings
        mappings = {
            'yyyy': '%Y',
            'yy': '%y',
            'MM': '%m',
            'dd': '%d',
            'HH': '%H',
            'mm': '%M',
            'ss': '%S',
            'SSS': '%f',
            'a': '%p',
        }

        result = nifi_format
        for nifi_pat, python_pat in mappings.items():
            result = result.replace(nifi_pat, python_pat)

        return result


@register_converter
class RouteOnAttributeConverter(ProcessorConverter):
    """
    Converter for RouteOnAttribute processor.

    RouteOnAttribute evaluates expressions and routes FlowFiles
    to different relationships based on the results.
    """

    processor_types = ["org.apache.nifi.processors.standard.RouteOnAttribute"]

    def convert(self, processor: Processor) -> ConversionResult:
        """
        Convert RouteOnAttribute processor to Python routing code.

        Generates code that evaluates routing conditions and returns
        FlowFile on the appropriate relationship.

        Args:
            processor: RouteOnAttribute processor to convert

        Returns:
            ConversionResult with generated routing code
        """
        function_name = self.generate_function_name(processor)

        # Get routing strategy
        routing_strategy = processor.get_property('Routing Strategy', 'Route to Property name')

        # Get routing rules (excluding special properties)
        routing_rules = {}
        for key, value in processor.properties.items():
            if key not in ['Routing Strategy'] and value:
                routing_rules[key] = value

        # Generate routing conditions
        condition_lines = []
        dependencies = {'typing', 'nifi2py.models'}

        for route_name, condition in routing_rules.items():
            python_condition = self._el_condition_to_python(condition)

            # Track dependencies
            if 'datetime' in python_condition:
                dependencies.add('datetime')

            condition_lines.append(
                f"    if {python_condition}:\n"
                f"        return {{'{route_name}': [flowfile]}}"
            )

        # Build imports
        import_lines = ['from typing import Dict, List', 'from nifi2py.models import FlowFile']
        if 'datetime' in dependencies:
            import_lines.append('from datetime import datetime')

        imports = '\n'.join(import_lines)

        # Build condition code
        if condition_lines:
            conditions = '\n\n'.join(condition_lines)
        else:
            conditions = '    # No routing rules configured'

        # Build function code
        code = f'''{imports}


def {function_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
{self.generate_docstring(processor)}
    # Get attributes for expression evaluation
    attributes = flowfile.attributes

    # Evaluate routing conditions
{conditions}

    # No conditions matched - route to unmatched
    return {{"unmatched": [flowfile]}}'''

        notes = f"Converted {len(routing_rules)} routing rule(s)"
        if len(routing_rules) == 0:
            notes += " - Warning: No routing rules configured"

        return ConversionResult(
            processor_id=processor.id,
            processor_name=processor.name,
            processor_type=processor.type,
            function_name=function_name,
            function_code=code,
            is_stub=False,
            dependencies=list(dependencies),
            notes=notes,
            coverage_percentage=85,
            warnings=["Complex EL conditions may need manual review"]
        )

    def _el_condition_to_python(self, condition: str) -> str:
        """
        Convert NiFi EL condition to Python boolean expression.

        Args:
            condition: NiFi EL condition

        Returns:
            Python boolean expression
        """
        # Remove ${ } wrapper if present
        condition = condition.strip()
        if condition.startswith('${') and condition.endswith('}'):
            condition = condition[2:-1]

        # Handle common patterns
        import re

        # Handle equals() function
        if ':equals(' in condition:
            match = re.match(r'([^:]+):equals\(([^)]+)\)', condition)
            if match:
                attr_name = match.group(1)
                value = match.group(2)
                # Remove quotes if present
                value = value.strip("'\"")
                return f"attributes.get('{attr_name}', '') == '{value}'"

        # Handle endsWith() function
        if ':endsWith(' in condition:
            match = re.match(r'([^:]+):endsWith\(([^)]+)\)', condition)
            if match:
                attr_name = match.group(1)
                value = match.group(2).strip("'\"")
                return f"attributes.get('{attr_name}', '').endswith('{value}')"

        # Handle startsWith() function
        if ':startsWith(' in condition:
            match = re.match(r'([^:]+):startsWith\(([^)]+)\)', condition)
            if match:
                attr_name = match.group(1)
                value = match.group(2).strip("'\"")
                return f"attributes.get('{attr_name}', '').startswith('{value}')"

        # Handle contains() function
        if ':contains(' in condition:
            match = re.match(r'([^:]+):contains\(([^)]+)\)', condition)
            if match:
                attr_name = match.group(1)
                value = match.group(2).strip("'\"")
                return f"'{value}' in attributes.get('{attr_name}', '')"

        # Handle isEmpty() function
        if ':isEmpty()' in condition:
            attr_name = condition.replace(':isEmpty()', '')
            return f"not attributes.get('{attr_name}', '')"

        # Handle simple attribute reference (truthy check)
        if ':' not in condition:
            return f"bool(attributes.get('{condition}', ''))"

        # Fallback - needs manual review
        return f"# TODO: Review condition - {repr(condition)}\n    False"
