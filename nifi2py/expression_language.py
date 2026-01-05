"""
NiFi Expression Language to Python Transpiler

This module provides transpilation from Apache NiFi Expression Language (EL)
to equivalent Python code. It uses Lark parser for proper AST-based parsing.

Example:
    >>> transpiler = ELTranspiler()
    >>> python_code = transpiler.transpile("${filename:toUpper()}")
    >>> print(python_code)
    attributes.get('filename', '').upper()
"""

import re
import uuid as uuid_module
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Union

from lark import Lark, Transformer, Token
from lark.exceptions import LarkError


# Helper functions for NiFi EL semantics
def _get_attr(attributes: dict, name: str, default: str = '') -> str:
    """Null-safe attribute getter matching NiFi semantics."""
    value = attributes.get(name, default)
    return '' if value is None else str(value)


def _to_number(value: Union[str, int, float]) -> Union[int, float]:
    """Convert string to int or float based on presence of decimal point."""
    if isinstance(value, (int, float)):
        return value
    if not value:
        return 0
    value_str = str(value)
    if '.' in value_str or 'e' in value_str.lower():
        return float(value_str)
    return int(value_str)


def _substring_before(text: str, delimiter: str) -> str:
    """Return substring before first occurrence of delimiter."""
    if not delimiter or not text:
        return text
    idx = text.find(delimiter)
    return text[:idx] if idx >= 0 else text


def _substring_after(text: str, delimiter: str) -> str:
    """Return substring after first occurrence of delimiter."""
    if not delimiter or not text:
        return text
    idx = text.find(delimiter)
    return text[idx + len(delimiter):] if idx >= 0 else text


def _substring_before_last(text: str, delimiter: str) -> str:
    """Return substring before last occurrence of delimiter."""
    if not delimiter or not text:
        return text
    idx = text.rfind(delimiter)
    return text[:idx] if idx >= 0 else text


def _substring_after_last(text: str, delimiter: str) -> str:
    """Return substring after last occurrence of delimiter."""
    if not delimiter or not text:
        return text
    idx = text.rfind(delimiter)
    return text[idx + len(delimiter):] if idx >= 0 else text


def _is_empty(value: str) -> bool:
    """Check if value is null, empty, or whitespace."""
    return not bool(value and value.strip())


def _pad_left(text: str, length: int, pad_char: str) -> str:
    """Pad string on left to specified length."""
    return text.rjust(length, pad_char)


def _pad_right(text: str, length: int, pad_char: str) -> str:
    """Pad string on right to specified length."""
    return text.ljust(length, pad_char)


def _convert_date_format(java_format: str) -> str:
    """Convert Java SimpleDateFormat to Python strftime format."""
    conversions = {
        'yyyy': '%Y',
        'yy': '%y',
        'MMMM': '%B',
        'MMM': '%b',
        'MM': '%m',
        'dd': '%d',
        'HH': '%H',
        'hh': '%I',
        'mm': '%M',
        'ss': '%S',
        'SSS': '%f',  # Note: milliseconds vs microseconds
        'a': '%p',
        'EEEE': '%A',
        'EEE': '%a',
        'D': '%j',
        'z': '%Z',
        'Z': '%z',
    }

    result = java_format
    # Replace in order of length (longest first to avoid partial matches)
    for java_pat, python_pat in sorted(conversions.items(), key=lambda x: -len(x[0])):
        result = result.replace(java_pat, python_pat)

    # Handle quoted literals: 'T' → T
    result = re.sub(r"'([^']*)'", r'\1', result)

    return result


class ELToPythonTransformer(Transformer):
    """Transform Lark parse tree to Python code."""

    def __init__(self):
        super().__init__()
        self.current_subject = None

    def _filter_tokens(self, items):
        """Filter out Lark tokens from items list."""
        return [item for item in items if not isinstance(item, Token)]

    def expression(self, items):
        """Process complete expression: ${subject:func():func()}"""
        # Filter out tokens (DOLLAR, LBRACE, RBRACE, COLON)
        # Only keep actual values (strings) and callables (functions)
        filtered = self._filter_tokens(items)

        if not filtered:
            return "''"

        subject = filtered[0]
        self.current_subject = subject

        # Apply function chain
        for func in filtered[1:]:
            if callable(func):
                subject = func(subject)
                self.current_subject = subject

        return subject

    def attribute_ref(self, items):
        """Process attribute reference: ${attr}"""
        attr_name = items[0]

        # Handle nested expression
        if isinstance(attr_name, str) and not attr_name.startswith('attributes.'):
            # It's a plain attribute name
            attr_name = attr_name.strip('"').strip("'")
            return f"attributes.get('{attr_name}', '')"
        else:
            # It's already an expression
            return attr_name

    def ATTRIBUTE_NAME(self, token):
        """Process attribute name token."""
        return str(token)

    def STRING_LITERAL(self, token):
        """Process string literal, handling escape sequences."""
        value = str(token)[1:-1]  # Remove quotes
        # Handle escape sequences
        value = value.replace('\\n', '\n')
        value = value.replace('\\t', '\t')
        value = value.replace('\\r', '\r')
        value = value.replace('\\\\', '\\')
        value = value.replace('\\"', '"')
        value = value.replace("\\'", "'")
        return value

    def NUMBER(self, token):
        """Process numeric literal."""
        return str(token)

    def BOOLEAN(self, token):
        """Process boolean literal."""
        return 'True' if str(token) == 'true' else 'False'

    # Argument handlers
    def number_arg(self, items):
        return items[0]

    def string_arg(self, items):
        return f"'{items[0]}'"

    def boolean_arg(self, items):
        return items[0]

    def expression_arg(self, items):
        # For nested expressions, items[0] is the already-transpiled expression
        return items[0] if items else ""

    # Standalone functions
    def uuid_func(self, items):
        return "str(uuid.uuid4())"

    def now_func(self, items):
        return "datetime.now()"

    def literal_func(self, items):
        filtered = self._filter_tokens(items)
        return filtered[0] if filtered else ""

    # String functions
    def to_upper(self, items):
        return lambda subject: f"({subject}).upper()"

    def to_lower(self, items):
        return lambda subject: f"({subject}).lower()"

    def trim(self, items):
        return lambda subject: f"({subject}).strip()"

    def substring(self, items):
        filtered = self._filter_tokens(items)
        start, end = filtered
        return lambda subject: f"({subject})[{start}:{end}]"

    def substring_before(self, items):
        filtered = self._filter_tokens(items)
        delimiter = filtered[0]
        return lambda subject: f"_substring_before(({subject}), {delimiter})"

    def substring_after(self, items):
        filtered = self._filter_tokens(items)
        delimiter = filtered[0]
        return lambda subject: f"_substring_after(({subject}), {delimiter})"

    def substring_before_last(self, items):
        filtered = self._filter_tokens(items)
        delimiter = filtered[0]
        return lambda subject: f"_substring_before_last(({subject}), {delimiter})"

    def substring_after_last(self, items):
        filtered = self._filter_tokens(items)
        delimiter = filtered[0]
        return lambda subject: f"_substring_after_last(({subject}), {delimiter})"

    def append(self, items):
        filtered = self._filter_tokens(items)
        suffix = filtered[0]
        return lambda subject: f"({subject}) + {suffix}"

    def prepend(self, items):
        filtered = self._filter_tokens(items)
        prefix = filtered[0]
        return lambda subject: f"{prefix} + ({subject})"

    def replace(self, items):
        filtered = self._filter_tokens(items)
        old, new = filtered
        return lambda subject: f"({subject}).replace({old}, {new})"

    def replace_all(self, items):
        filtered = self._filter_tokens(items)
        pattern, replacement = filtered
        return lambda subject: f"re.sub({pattern}, {replacement}, ({subject}))"

    def replace_first(self, items):
        filtered = self._filter_tokens(items)
        pattern, replacement = filtered
        return lambda subject: f"re.sub({pattern}, {replacement}, ({subject}), count=1)"

    def replace_null(self, items):
        filtered = self._filter_tokens(items)
        default = filtered[0]
        # For replaceNull, we need to check if attribute exists
        # This is tricky in the current structure - we'll handle it specially
        return lambda subject: subject  # Will be handled at attribute_ref level

    def replace_empty(self, items):
        filtered = self._filter_tokens(items)
        default = filtered[0]
        return lambda subject: f"({subject}) or {default}"

    def index_of(self, items):
        filtered = self._filter_tokens(items)
        search = filtered[0]
        return lambda subject: f"({subject}).find({search})"

    def last_index_of(self, items):
        filtered = self._filter_tokens(items)
        search = filtered[0]
        return lambda subject: f"({subject}).rfind({search})"

    def pad_left(self, items):
        filtered = self._filter_tokens(items)
        length, pad_char = filtered
        return lambda subject: f"_pad_left(({subject}), {length}, {pad_char})"

    def pad_right(self, items):
        filtered = self._filter_tokens(items)
        length, pad_char = filtered
        return lambda subject: f"_pad_right(({subject}), {length}, {pad_char})"

    def evaluate_el_string(self, items):
        """Evaluate EL expressions within the attribute value."""
        # This requires recursive transpilation - return a special marker
        return lambda subject: f"_evaluate_el_string(({subject}), attributes)"

    # Boolean functions
    def is_empty(self, items):
        return lambda subject: f"_is_empty(({subject}))"

    def is_null(self, items):
        # isNull checks if attribute doesn't exist or is None
        # After attributes.get(), we get '' for missing, so check for that
        return lambda subject: f"(({subject}) == '' or ({subject}) is None)"

    def not_null(self, items):
        # notNull is opposite of isNull - true if attribute exists and has value
        # In NiFi, empty string is considered "not null" (attribute exists)
        # But since we can't distinguish missing vs empty, we check != None
        return lambda subject: f"({subject}) is not None"

    def equals(self, items):
        # Filter out tokens
        filtered = self._filter_tokens(items)
        value = filtered[0] if filtered else ""
        return lambda subject: f"({subject}) == {value}"

    def equals_ignore_case(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"({subject}).lower() == ({value}).lower()"

    def starts_with(self, items):
        filtered = self._filter_tokens(items)
        prefix = filtered[0]
        return lambda subject: f"({subject}).startswith({prefix})"

    def ends_with(self, items):
        filtered = self._filter_tokens(items)
        suffix = filtered[0]
        return lambda subject: f"({subject}).endswith({suffix})"

    def contains(self, items):
        filtered = self._filter_tokens(items)
        substring = filtered[0]
        return lambda subject: f"{substring} in ({subject})"

    def matches(self, items):
        filtered = self._filter_tokens(items)
        pattern = filtered[0]
        return lambda subject: f"bool(re.match({pattern}, ({subject})))"

    def find(self, items):
        filtered = self._filter_tokens(items)
        pattern = filtered[0]
        return lambda subject: f"bool(re.search({pattern}, ({subject})))"

    def and_op(self, items):
        filtered = self._filter_tokens(items)
        other = filtered[0]
        return lambda subject: f"({subject}) and ({other})"

    def or_op(self, items):
        filtered = self._filter_tokens(items)
        other = filtered[0]
        return lambda subject: f"({subject}) or ({other})"

    def not_op(self, items):
        return lambda subject: f"not ({subject})"

    def if_else(self, items):
        filtered = self._filter_tokens(items)
        true_val, false_val = filtered
        return lambda subject: f"{true_val} if ({subject}) else {false_val}"

    def in_op(self, items):
        filtered = self._filter_tokens(items)
        values = filtered
        values_list = '[' + ', '.join(str(v) for v in values) + ']'
        return lambda subject: f"({subject}) in {values_list}"

    # Numeric functions
    def length(self, items):
        return lambda subject: f"len({subject})"

    def to_number(self, items):
        return lambda subject: f"_to_number({subject})"

    def to_decimal(self, items):
        return lambda subject: f"float({subject})"

    def plus(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) + _to_number({value})"

    def minus(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) - _to_number({value})"

    def multiply(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) * _to_number({value})"

    def divide(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) / _to_number({value})"

    def mod(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) % _to_number({value})"

    def gt(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) > _to_number({value})"

    def lt(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) < _to_number({value})"

    def ge(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) >= _to_number({value})"

    def le(self, items):
        filtered = self._filter_tokens(items)
        value = filtered[0]
        return lambda subject: f"_to_number({subject}) <= _to_number({value})"

    def math_func(self, items):
        filtered = self._filter_tokens(items)
        func_name = filtered[0].strip("'").strip('"')
        if func_name == 'abs':
            return lambda subject: f"abs(_to_number({subject}))"
        elif func_name == 'ceil':
            return lambda subject: f"math.ceil(_to_number({subject}))"
        elif func_name == 'floor':
            return lambda subject: f"math.floor(_to_number({subject}))"
        elif func_name == 'round':
            return lambda subject: f"round(_to_number({subject}))"
        else:
            raise ValueError(f"Unsupported math function: {func_name}")

    # Date functions
    def format_date(self, items):
        filtered = self._filter_tokens(items)
        format_str = filtered[0].strip("'").strip('"')
        python_format = _convert_date_format(format_str)
        return lambda subject: f"({subject}).strftime('{python_format}')"

    def to_date_no_args(self, items):
        # toDate() with no args - convert timestamp millis to datetime
        return lambda subject: f"datetime.fromtimestamp(_to_number({subject}) / 1000)"

    def to_date(self, items):
        filtered = self._filter_tokens(items)
        # toDate with format string - parse string to timestamp
        format_str = filtered[0].strip("'").strip('"')
        python_format = _convert_date_format(format_str)
        return lambda subject: f"datetime.strptime(({subject}), '{python_format}').timestamp() * 1000"

    # Special multi-value functions
    def all_attributes(self, items):
        filtered = self._filter_tokens(items)
        attr_names = [name.strip("'").strip('"') for name in filtered]
        names_list = '[' + ', '.join(f"'{name}'" for name in attr_names) + ']'
        return f"[attributes.get(k, '') for k in {names_list}]"

    def all_matching_attributes(self, items):
        filtered = self._filter_tokens(items)
        pattern = filtered[0].strip("'").strip('"')
        return f"[v for k, v in attributes.items() if re.match(r'{pattern}', k)]"

    def join(self, items):
        filtered = self._filter_tokens(items)
        delimiter = filtered[0]
        return lambda subject: f"{delimiter}.join({subject})"

    def count(self, items):
        return lambda subject: f"len({subject})"


class ELTranspiler:
    """
    Main transpiler class for NiFi Expression Language to Python.

    Example:
        >>> transpiler = ELTranspiler()
        >>> code = transpiler.transpile("${filename:toUpper()}")
        >>> print(code)
        attributes.get('filename', '').upper()
    """

    def __init__(self):
        """Initialize the transpiler with Lark parser."""
        grammar_path = Path(__file__).parent / 'el_grammar.lark'
        with open(grammar_path, 'r') as f:
            grammar = f.read()
        self.parser = Lark(grammar, start='start', parser='lalr')
        self.transformer = ELToPythonTransformer()

    def transpile(self, el_expression: str) -> str:
        """
        Transpile a NiFi EL expression to Python code.

        Args:
            el_expression: NiFi expression like "${attr:toUpper()}"

        Returns:
            Python code string

        Raises:
            ValueError: If expression is invalid
        """
        if not el_expression:
            return "''"

        # Handle literal strings without EL
        if not ('${' in el_expression):
            return f"'{el_expression}'"

        try:
            # Parse to AST
            tree = self.parser.parse(el_expression)

            # Transform to Python
            python_code = self.transformer.transform(tree)

            return python_code
        except LarkError as e:
            raise ValueError(f"Failed to parse expression '{el_expression}': {e}")

    def transpile_embedded(self, text: str) -> str:
        """
        Handle text with embedded EL expressions.

        Example:
            >>> transpiler.transpile_embedded("file_${uuid()}_${now():format('yyyyMMdd')}.txt")
            "f'file_{str(uuid.uuid4())}_{datetime.now().strftime(\"%Y%m%d\")}.txt'"

        Args:
            text: Text containing zero or more ${...} expressions

        Returns:
            Python f-string or regular string
        """
        if not text:
            return "''"

        # Handle escaped $$
        text = text.replace('$$', '\x00')  # Temporary marker

        # Find all ${...} expressions
        pattern = r'\$\{[^}]+\}'
        matches = list(re.finditer(pattern, text))

        if not matches:
            # No EL expressions - return as literal
            text = text.replace('\x00', '$')
            return f"'{text}'"

        # Build f-string
        parts = []
        last_end = 0

        for match in matches:
            # Add literal part before expression
            if match.start() > last_end:
                literal = text[last_end:match.start()]
                literal = literal.replace('\x00', '$')
                parts.append(literal)

            # Transpile expression and add as interpolation
            el_expr = match.group()
            python_expr = self.transpile(el_expr)
            parts.append('{' + python_expr + '}')

            last_end = match.end()

        # Add remaining literal
        if last_end < len(text):
            literal = text[last_end:]
            literal = literal.replace('\x00', '$')
            parts.append(literal)

        # Build f-string
        result = 'f"' + ''.join(parts) + '"'
        return result


# Helper function for evaluateELString
def _evaluate_el_string(text: str, attributes: dict) -> str:
    """
    Recursively evaluate EL expressions within a string.
    This is used by the evaluateELString() function.
    """
    transpiler = ELTranspiler()
    return eval(transpiler.transpile_embedded(text), {
        'attributes': attributes,
        'datetime': datetime,
        'uuid': uuid_module,
        're': re,
        '_substring_before': _substring_before,
        '_substring_after': _substring_after,
        '_substring_before_last': _substring_before_last,
        '_substring_after_last': _substring_after_last,
        '_is_empty': _is_empty,
        '_to_number': _to_number,
        '_pad_left': _pad_left,
        '_pad_right': _pad_right,
        '_evaluate_el_string': _evaluate_el_string,
    })


# Export all helper functions and main class
__all__ = [
    'ELTranspiler',
    '_get_attr',
    '_to_number',
    '_substring_before',
    '_substring_after',
    '_substring_before_last',
    '_substring_after_last',
    '_is_empty',
    '_pad_left',
    '_pad_right',
    '_convert_date_format',
    '_evaluate_el_string',
]
