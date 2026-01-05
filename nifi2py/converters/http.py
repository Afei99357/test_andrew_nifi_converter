"""
Converters for HTTP-related processors.

This module provides converters for processors that perform HTTP operations
like InvokeHTTP, GetHTTP, etc.
"""

import textwrap
import re
from typing import Dict, List

from nifi2py.models import Processor, ConversionResult
from nifi2py.converters.base import ProcessorConverter, register_converter


@register_converter
class InvokeHTTPConverter(ProcessorConverter):
    """
    Converter for InvokeHTTP processor.

    InvokeHTTP sends HTTP requests and captures responses.
    It creates relationships for Original, Response, Retry, Failure, and No Retry.
    """

    processor_types = ["org.apache.nifi.processors.standard.InvokeHTTP"]

    def convert(self, processor: Processor) -> ConversionResult:
        """
        Convert InvokeHTTP processor to Python requests code.

        Properties used:
        - HTTP Method: GET, POST, PUT, DELETE, etc.
        - Remote URL: URL to invoke (supports EL)
        - Connection Timeout: Connection timeout
        - Read Timeout: Read timeout
        - Follow Redirects: Whether to follow redirects
        - Attributes to Send: Regex for attributes to send as headers
        - SSL Context Service: SSL configuration (stub for now)

        Args:
            processor: InvokeHTTP processor to convert

        Returns:
            ConversionResult with generated HTTP invocation code
        """
        function_name = self.generate_function_name(processor)

        # Get processor properties
        http_method = processor.get_property('HTTP Method', 'GET').upper()
        remote_url = processor.get_property('Remote URL', 'http://localhost')
        connection_timeout = processor.get_property('Connection Timeout', '5 sec')
        read_timeout = processor.get_property('Read Timeout', '15 sec')
        follow_redirects = processor.get_property('Follow Redirects', 'true')
        attributes_to_send = processor.get_property('Attributes to Send', '')

        # Parse timeouts to seconds
        connect_timeout_sec = self._parse_timeout(connection_timeout)
        read_timeout_sec = self._parse_timeout(read_timeout)

        # Convert URL to Python expression (handle EL)
        url_expr = self._simple_el_to_python(remote_url)

        # Determine if we should follow redirects
        follow_redirects_bool = follow_redirects.lower() in ['true', 'yes', '1']

        # Build attributes to headers logic
        if attributes_to_send:
            headers_code = textwrap.indent(textwrap.dedent(f'''
                # Convert matching attributes to headers
                headers = {{}}
                pattern = re.compile(r'{attributes_to_send}')
                for key, value in attributes.items():
                    if pattern.match(key):
                        headers[key] = value
            ''').strip(), '    ')
        else:
            headers_code = "    headers = {}"

        # Build function code
        code = f'''import re
import requests
from typing import Dict, List
from nifi2py.models import FlowFile


def {function_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
{self.generate_docstring(processor)}
    # Get attributes for expression evaluation
    attributes = flowfile.attributes

    # Prepare request
    url = {url_expr}
    method = '{http_method}'

{headers_code}

    # Set timeouts
    timeout = ({connect_timeout_sec}, {read_timeout_sec})  # (connect, read)

    try:
        # Make HTTP request
        if method == 'GET':
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects={follow_redirects_bool}
            )
        elif method == 'POST':
            response = requests.post(
                url,
                data=flowfile.content,
                headers=headers,
                timeout=timeout,
                allow_redirects={follow_redirects_bool}
            )
        elif method == 'PUT':
            response = requests.put(
                url,
                data=flowfile.content,
                headers=headers,
                timeout=timeout,
                allow_redirects={follow_redirects_bool}
            )
        elif method == 'DELETE':
            response = requests.delete(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects={follow_redirects_bool}
            )
        else:
            # Other methods
            response = requests.request(
                method,
                url,
                data=flowfile.content,
                headers=headers,
                timeout=timeout,
                allow_redirects={follow_redirects_bool}
            )

        # Create response FlowFile
        response_flowfile = flowfile.clone(
            content=response.content,
            attributes=attributes.copy()
        )

        # Add response attributes
        response_flowfile.attributes['invokehttp.status.code'] = str(response.status_code)
        response_flowfile.attributes['invokehttp.status.message'] = response.reason
        response_flowfile.attributes['invokehttp.request.url'] = url
        response_flowfile.attributes['invokehttp.tx.id'] = response_flowfile.uuid

        # Add response headers as attributes
        for header_name, header_value in response.headers.items():
            response_flowfile.attributes[f'invokehttp.response.header.{{header_name}}'] = header_value

        # Determine routing based on status code
        if response.status_code >= 200 and response.status_code < 300:
            # Success - return both Original and Response
            return {{
                "Original": [flowfile],
                "Response": [response_flowfile]
            }}
        elif response.status_code >= 500:
            # Server error - route to Retry
            return {{
                "Retry": [flowfile]
            }}
        else:
            # Client error - route to No Retry
            return {{
                "No Retry": [flowfile]
            }}

    except requests.exceptions.Timeout:
        # Timeout - route to Retry
        flowfile.attributes['invokehttp.error.message'] = 'Request timeout'
        return {{"Retry": [flowfile]}}

    except requests.exceptions.RequestException as e:
        # Other request errors - route to Failure
        flowfile.attributes['invokehttp.error.message'] = str(e)
        return {{"Failure": [flowfile]}}'''

        warnings = []
        if '${' in remote_url:
            warnings.append("Remote URL contains EL expressions - verify correct conversion")
        if processor.get_property('SSL Context Service'):
            warnings.append("SSL Context Service not implemented - SSL configuration may need manual setup")

        return ConversionResult(
            processor_id=processor.id,
            processor_name=processor.name,
            processor_type=processor.type,
            function_name=function_name,
            function_code=code,
            is_stub=False,
            dependencies=['re', 'requests', 'typing', 'nifi2py.models'],
            notes="Successfully converted InvokeHTTP processor",
            coverage_percentage=85,
            warnings=warnings
        )

    def _parse_timeout(self, timeout_str: str) -> int:
        """
        Parse timeout string to seconds.

        Args:
            timeout_str: Timeout string like "5 sec", "30 seconds", "1 min"

        Returns:
            Timeout in seconds
        """
        timeout_str = timeout_str.strip().lower()

        # Extract number and unit
        match = re.match(r'(\d+(?:\.\d+)?)\s*(sec|second|seconds|min|minute|minutes|ms|millis|milliseconds)?', timeout_str)
        if not match:
            return 5  # Default to 5 seconds

        value = float(match.group(1))
        unit = match.group(2) or 'sec'

        # Convert to seconds
        if unit in ['ms', 'millis', 'milliseconds']:
            return int(value / 1000)
        elif unit in ['min', 'minute', 'minutes']:
            return int(value * 60)
        else:  # seconds
            return int(value)

    def _simple_el_to_python(self, expression: str) -> str:
        """
        Simple EL to Python converter for URL expressions.

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

        # Handle embedded expressions in URLs
        def replace_el(match):
            expr = match.group(1)

            # Handle simple attribute reference
            if ':' not in expr:
                return "{attributes.get('" + expr + "', '')}"

            # Handle attribute with functions (simplified)
            attr_name = expr.split(':')[0]
            return "{attributes.get('" + attr_name + "', '')}"

        result = re.sub(r'\$\{([^}]+)\}', replace_el, expression)
        return f'f"{result}"'
