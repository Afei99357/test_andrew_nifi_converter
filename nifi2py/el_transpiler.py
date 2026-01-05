#!/usr/bin/env python3
"""
Enhanced Expression Language Transpiler

Converts NiFi Expression Language (EL) to Python code.
Supports common patterns needed for UpdateAttribute and RouteOnAttribute.
"""

import re
from typing import Optional


class ELTranspiler:
    """Transpile NiFi Expression Language to Python"""

    def __init__(self):
        """Initialize transpiler with function mappings"""
        # String function mappings
        self.string_functions = {
            'toUpper': 'upper',
            'toLower': 'lower',
            'trim': 'strip',
            'length': '__len__',
        }

        # Boolean comparison mappings
        self.comparisons = {
            'gt': '>',
            'ge': '>=',
            'lt': '<',
            'le': '<=',
            'equals': '==',
            'notEquals': '!=',
        }

    def transpile(self, expression: str, context: str = 'flowfile') -> str:
        """
        Transpile NiFi EL to Python

        Args:
            expression: NiFi EL expression (e.g., "${filename:toUpper()}")
            context: Context for attribute access ('flowfile' or 'attributes')

        Returns:
            Python code as string
        """
        if not expression:
            return "''"

        # Plain string (no EL)
        if not ('${' in expression):
            return f"'{expression}'"

        # Extract EL expression(s)
        if expression.startswith('${') and expression.endswith('}'):
            # Single EL expression: ${...}
            return self._transpile_single_el(expression[2:-1], context)
        else:
            # Embedded EL: "prefix_${expr}_suffix"
            return self._transpile_embedded_el(expression, context)

    def _transpile_single_el(self, el_expr: str, context: str) -> str:
        """Transpile a single EL expression (without ${ })"""

        # Handle literals
        if el_expr.startswith("'") and el_expr.endswith("'"):
            return el_expr

        # Handle now()
        if el_expr == 'now()':
            return 'datetime.now().isoformat()'

        # Handle uuid()
        if el_expr == 'uuid()':
            return 'str(uuid.uuid4())'

        # Handle attribute with method chain: ${filename:toUpper():trim()}
        if ':' in el_expr:
            return self._transpile_method_chain(el_expr, context)

        # Handle function calls with parameters: ${filename:substring(0, 5)}
        if '(' in el_expr and ')' in el_expr:
            return self._transpile_function_call(el_expr, context)

        # Simple attribute reference: ${filename}
        if context == 'flowfile':
            return f"flowfile.attributes.get('{el_expr}', '')"
        else:
            return f"attributes.get('{el_expr}', '')"

    def _transpile_method_chain(self, el_expr: str, context: str) -> str:
        """Transpile method chain: filename:toUpper():trim()"""
        parts = el_expr.split(':')
        attr_name = parts[0]
        methods = parts[1:]

        # Start with attribute access
        if context == 'flowfile':
            result = f"flowfile.attributes.get('{attr_name}', '')"
        else:
            result = f"attributes.get('{attr_name}', '')"

        # Apply each method
        for method in methods:
            result = self._apply_method(result, method)

        return result

    def _apply_method(self, expr: str, method: str) -> str:
        """Apply a single method to an expression"""

        # Simple string methods: toUpper(), toLower(), trim()
        if method in ('toUpper()', 'toUpper'):
            return f"{expr}.upper()"
        elif method in ('toLower()', 'toLower'):
            return f"{expr}.lower()"
        elif method in ('trim()', 'trim'):
            return f"{expr}.strip()"
        elif method in ('length()', 'length'):
            return f"len({expr})"

        # substring(start, end)
        if method.startswith('substring('):
            args = self._extract_args(method)
            if len(args) == 1:
                return f"{expr}[{args[0]}:]"
            elif len(args) == 2:
                return f"{expr}[{args[0]}:{args[1]}]"

        # substringBefore(delimiter)
        if method.startswith('substringBefore('):
            args = self._extract_args(method)
            delimiter = args[0]
            return f"{expr}.split({delimiter})[0] if {delimiter} in {expr} else {expr}"

        # substringAfter(delimiter)
        if method.startswith('substringAfter('):
            args = self._extract_args(method)
            delimiter = args[0]
            return f"{expr}.split({delimiter}, 1)[1] if {delimiter} in {expr} else ''"

        # replace(find, replace)
        if method.startswith('replace('):
            args = self._extract_args(method)
            if len(args) == 2:
                return f"{expr}.replace({args[0]}, {args[1]})"

        # replaceAll(regex, replacement)
        if method.startswith('replaceAll('):
            args = self._extract_args(method)
            if len(args) == 2:
                return f"re.sub({args[0]}, {args[1]}, {expr})"

        # contains(substring)
        if method.startswith('contains('):
            args = self._extract_args(method)
            return f"({args[0]} in {expr})"

        # startsWith(prefix)
        if method.startswith('startsWith('):
            args = self._extract_args(method)
            return f"{expr}.startswith({args[0]})"

        # endsWith(suffix)
        if method.startswith('endsWith('):
            args = self._extract_args(method)
            return f"{expr}.endswith({args[0]})"

        # matches(regex)
        if method.startswith('matches('):
            args = self._extract_args(method)
            return f"bool(re.match({args[0]}, {expr}))"

        # format(pattern) for dates
        if method.startswith('format('):
            args = self._extract_args(method)
            if args:
                java_pattern = args[0].strip("'\"")
                python_pattern = self._convert_date_format(java_pattern)
                return f"{expr}.strftime('{python_pattern}')"

        # Boolean methods
        if method in ('isEmpty()', 'isEmpty'):
            return f"not bool({expr})"
        elif method in ('notEmpty()', 'notEmpty'):
            return f"bool({expr})"

        # Comparison methods: gt(value), lt(value), etc.
        for comp_name, comp_op in self.comparisons.items():
            if method.startswith(f'{comp_name}('):
                args = self._extract_args(method)
                if args:
                    return f"({expr} {comp_op} {args[0]})"

        # Unknown method - return as-is with comment
        return f"{expr}  # TODO: Transpile {method}"

    def _transpile_function_call(self, el_expr: str, context: str) -> str:
        """Transpile function call: substring(filename, 0, 5)"""
        # This is for functions called on attributes
        # For now, delegate to method chain handler
        return self._transpile_method_chain(el_expr, context)

    def _transpile_embedded_el(self, expression: str, context: str) -> str:
        """
        Transpile embedded EL: "prefix_${expr}_suffix"

        Returns: f"prefix_{expr}_suffix"
        """
        # Find all ${...} patterns
        pattern = r'\$\{([^}]+)\}'

        def replace_el(match):
            el_expr = match.group(1)
            return "{" + self._transpile_single_el(el_expr, context) + "}"

        # Replace ${...} with {...}
        result = re.sub(pattern, replace_el, expression)

        # Wrap in f-string
        return f'f"{result}"'

    def _extract_args(self, method: str) -> list:
        """Extract arguments from method call: substring(0, 5) → ['0', '5']"""
        match = re.match(r'\w+\((.*)\)', method)
        if match:
            args_str = match.group(1)
            if not args_str:
                return []

            # Simple split by comma (doesn't handle nested calls)
            args = [arg.strip() for arg in args_str.split(',')]
            return args
        return []

    def _convert_date_format(self, java_pattern: str) -> str:
        """
        Convert Java date format to Python strftime format

        Java: yyyy-MM-dd HH:mm:ss
        Python: %Y-%m-%d %H:%M:%S
        """
        conversions = {
            'yyyy': '%Y',
            'yy': '%y',
            'MM': '%m',
            'dd': '%d',
            'HH': '%H',
            'mm': '%M',
            'ss': '%S',
            'SSS': '%f',  # Milliseconds (Python uses microseconds)
            'a': '%p',    # AM/PM
        }

        result = java_pattern
        for java_fmt, python_fmt in conversions.items():
            result = result.replace(java_fmt, python_fmt)

        return result

    def transpile_boolean_expression(self, expression: str, context: str = 'flowfile') -> str:
        """
        Transpile boolean EL expression for RouteOnAttribute

        Examples:
          ${fileSize:gt(1000)} → int(flowfile.attributes.get('fileSize', '0')) > 1000
          ${filename:endsWith('.txt')} → flowfile.attributes.get('filename', '').endswith('.txt')
        """
        # Remove ${ } wrapper if present
        if expression.startswith('${') and expression.endswith('}'):
            expression = expression[2:-1]

        # Handle and(), or(), not()
        if expression.startswith('and('):
            # Extract sub-expressions
            # Simplified: and(expr1, expr2) → (expr1) and (expr2)
            inner = expression[4:-1]
            parts = self._split_args(inner)
            transpiled_parts = [self.transpile_boolean_expression(part, context) for part in parts]
            return '(' + ' and '.join(transpiled_parts) + ')'

        if expression.startswith('or('):
            inner = expression[3:-1]
            parts = self._split_args(inner)
            transpiled_parts = [self.transpile_boolean_expression(part, context) for part in parts]
            return '(' + ' or '.join(transpiled_parts) + ')'

        if expression.startswith('not('):
            inner = expression[4:-1]
            transpiled = self.transpile_boolean_expression(inner, context)
            return f'not ({transpiled})'

        # Handle method chain that returns boolean
        if ':' in expression:
            result = self._transpile_method_chain(expression, context)

            # If result contains comparison operators, it's already boolean
            if any(op in result for op in ['>', '<', '==', '!=', '>=', '<=', ' in ', 'startswith', 'endswith', 'match']):
                return result

            # Otherwise, wrap in bool()
            return f'bool({result})'

        # Simple attribute check: ${filename} → bool(flowfile.attributes.get('filename', ''))
        if context == 'flowfile':
            return f"bool(flowfile.attributes.get('{expression}', ''))"
        else:
            return f"bool(attributes.get('{expression}', ''))"

    def _split_args(self, args_str: str) -> list:
        """Split arguments respecting nested parentheses"""
        # Simplified version - doesn't handle all edge cases
        args = []
        current = []
        depth = 0

        for char in args_str:
            if char == '(' :
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                args.append(''.join(current).strip())
                current = []
            else:
                current.append(char)

        if current:
            args.append(''.join(current).strip())

        return args


# Global instance for easy import
el_transpiler = ELTranspiler()
