#!/usr/bin/env python3
"""
Processor-specific converters that generate functional Python code
from NiFi processor configurations.
"""

from typing import Dict, List, Any, Optional
import re
from .el_transpiler import ELTranspiler


class ProcessorConverter:
    """Base class for processor converters"""

    def __init__(self, processor_config: Dict[str, Any], provenance_events: List[Dict[str, Any]]):
        self.config = processor_config
        self.events = provenance_events
        self.component = processor_config['component']
        self.properties = self.component['config']['properties']
        self.processor_name = self.component['name']
        self.processor_id = self.component['id']
        self.el_transpiler = ELTranspiler()

    def transpile_el(self, expression: str) -> str:
        """Convert NiFi Expression Language to Python"""
        if not expression:
            return "''"

        # Simple EL transpilation for common patterns
        if expression.startswith('${') and expression.endswith('}'):
            # Extract the expression content
            el_expr = expression[2:-1]

            # Handle attribute references
            if ':' not in el_expr and '(' not in el_expr:
                # Simple attribute reference: ${url} → flowfile.attributes.get('url', '')
                return f"flowfile.attributes.get('{el_expr}', '')"

            # Handle now() function
            if el_expr == 'now()':
                return "datetime.now().isoformat()"

            # Handle attribute with method chain (e.g., ${url:toUpper()})
            if ':' in el_expr:
                parts = el_expr.split(':')
                attr_name = parts[0]
                methods = parts[1:]

                result = f"flowfile.attributes.get('{attr_name}', '')"
                for method in methods:
                    if method == 'toUpper()':
                        result = f"{result}.upper()"
                    elif method == 'toLower()':
                        result = f"{result}.lower()"
                    elif method == 'trim()':
                        result = f"{result}.strip()"

                return result

        # If it's a plain string, return it quoted
        return f"'{expression}'"

    def generate_function(self) -> str:
        """Generate Python function for this processor"""
        raise NotImplementedError("Subclasses must implement generate_function()")


class DetectDuplicateConverter(ProcessorConverter):
    """Converter for DetectDuplicate processor"""

    def generate_function(self) -> str:
        cache_identifier = self.properties.get('Cache Entry Identifier', '${uuid}')
        cache_key_expr = self.transpile_el(cache_identifier)

        code = []
        code.append(f"def process_detectduplicate_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile, seen_cache: set) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    DetectDuplicate: {self.processor_name}')
        code.append(f'    Cache Entry Identifier: {cache_identifier}')
        code.append(f'    """')
        code.append(f'    # Extract cache key from FlowFile')
        code.append(f'    cache_key = {cache_key_expr}')
        code.append(f'    ')
        code.append(f'    # Check if we\'ve seen this before')
        code.append(f'    if cache_key in seen_cache:')
        code.append(f'        # Duplicate - route to duplicate relationship')
        code.append(f'        return {{"duplicate": [flowfile]}}')
        code.append(f'    else:')
        code.append(f'        # New entry - add to cache and route to non-duplicate')
        code.append(f'        seen_cache.add(cache_key)')
        code.append(f'        return {{"non-duplicate": [flowfile]}}')

        return '\n'.join(code)


class ExtractTextConverter(ProcessorConverter):
    """Converter for ExtractText processor"""

    def generate_function(self) -> str:
        # Get all user-defined properties (not the built-in config ones)
        builtin_props = {
            'Character Set', 'Maximum Buffer Size', 'Maximum Capture Group Length',
            'Enable Canonical Equivalence', 'Enable Case-insensitive Matching',
            'Permit Whitespace and Comments in Pattern', 'Enable DOTALL Mode',
            'Enable Literal Parsing of the Pattern', 'Enable Multiline Mode',
            'Enable Unicode-aware Case Folding', 'Enable Unicode Predefined Character Classes',
            'Enable Unix Lines Mode', 'Include Capture Group 0',
            'extract-text-enable-repeating-capture-group', 'extract-text-enable-named-groups'
        }

        extraction_patterns = {
            k: v for k, v in self.properties.items()
            if k not in builtin_props and v
        }

        code = []
        code.append(f"def process_extracttext_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    ExtractText: {self.processor_name}')
        code.append(f'    Extracts text using regex patterns and sets attributes')
        code.append(f'    """')
        code.append(f'    # Get content as string')
        code.append(f'    content = flowfile.content.decode("utf-8") if isinstance(flowfile.content, bytes) else str(flowfile.content)')
        code.append(f'    ')
        code.append(f'    # Apply regex patterns to extract attributes')

        for attr_name, pattern in extraction_patterns.items():
            # Use the pattern as-is in a raw string
            code.append(f'    match = re.search(r\'{pattern}\', content)')
            code.append(f'    if match:')
            code.append(f'        flowfile.attributes["{attr_name}"] = match.group(1) if match.groups() else match.group(0)')

        code.append(f'    ')
        code.append(f'    return {{"success": [flowfile]}}')

        return '\n'.join(code)


class RouteTextConverter(ProcessorConverter):
    """Converter for RouteText processor"""

    def generate_function(self) -> str:
        routing_strategy = self.properties.get('Routing Strategy', 'Route to each matching Property Name')
        matching_strategy = self.properties.get('Matching Strategy', 'Contains Regular Expression')
        ignore_case = self.properties.get('Ignore Case', 'true').lower() == 'true'

        # Get routing rules (user-defined properties)
        builtin_props = {
            'Routing Strategy', 'Matching Strategy', 'Character Set',
            'Ignore Leading/Trailing Whitespace', 'Ignore Case'
        }

        routing_rules = {
            k: v for k, v in self.properties.items()
            if k not in builtin_props and v
        }

        code = []
        code.append(f"def process_routetext_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    RouteText: {self.processor_name}')
        code.append(f'    Routes based on pattern matching')
        code.append(f'    """')
        code.append(f'    # Get content as string')
        code.append(f'    content = flowfile.content.decode("utf-8") if isinstance(flowfile.content, bytes) else str(flowfile.content)')
        code.append(f'    ')
        code.append(f'    # Check each routing rule')

        if 'Contains Regular Expression' in matching_strategy:
            flags = 're.IGNORECASE' if ignore_case else '0'

            for rule_name, pattern in routing_rules.items():
                # Use the pattern as-is in a raw string
                code.append(f'    if re.search(r\'{pattern}\', content, {flags}):')
                code.append(f'        return {{"{rule_name}": [flowfile]}}')

            code.append(f'    ')
            code.append(f'    # No matches - route to unmatched')
            code.append(f'    return {{"unmatched": [flowfile]}}')

        return '\n'.join(code)


class SplitTextConverter(ProcessorConverter):
    """Converter for SplitText processor"""

    def generate_function(self) -> str:
        line_split_count = int(self.properties.get('Line Split Count', '1'))
        header_line_count = int(self.properties.get('Header Line Count', '0'))
        remove_trailing_newlines = self.properties.get('Remove Trailing Newlines', 'true').lower() == 'true'

        code = []
        code.append(f"def process_splittext_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    SplitText: {self.processor_name}')
        code.append(f'    Splits text content into multiple FlowFiles')
        code.append(f'    """')
        code.append(f'    # Get content as string')
        code.append(f'    content = flowfile.content.decode("utf-8") if isinstance(flowfile.content, bytes) else str(flowfile.content)')
        code.append(f'    lines = content.splitlines()')
        code.append(f'    ')
        code.append(f'    # Skip header lines')
        code.append(f'    lines = lines[{header_line_count}:]')
        code.append(f'    ')
        code.append(f'    # Split into chunks of {line_split_count} line(s)')
        code.append(f'    splits = []')
        code.append(f'    for i in range(0, len(lines), {line_split_count}):')
        code.append(f'        chunk_lines = lines[i:i+{line_split_count}]')
        code.append(f'        chunk_content = "\\n".join(chunk_lines)')
        code.append(f'        ')
        code.append(f'        # Create new FlowFile for this split')
        code.append(f'        split_ff = FlowFile(')
        code.append(f'            content=chunk_content.encode("utf-8"),')
        code.append(f'            attributes=flowfile.attributes.copy()')
        code.append(f'        )')
        code.append(f'        split_ff.attributes["fragment.index"] = str(i // {line_split_count})')
        code.append(f'        split_ff.attributes["fragment.count"] = str((len(lines) + {line_split_count} - 1) // {line_split_count})')
        code.append(f'        splits.append(split_ff)')
        code.append(f'    ')
        code.append(f'    return {{"splits": splits, "original": [flowfile]}}')

        return '\n'.join(code)


class GetHTTPConverter(ProcessorConverter):
    """Converter for GetHTTP processor"""

    def generate_function(self) -> str:
        url = self.properties.get('URL', '')
        filename = self.properties.get('Filename', 'download')
        connection_timeout = self.properties.get('Connection Timeout', '30 sec')

        code = []
        code.append(f"def process_gethttp_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    GetHTTP: {self.processor_name}')
        code.append(f'    Fetches content from HTTP URL')
        code.append(f'    """')
        code.append(f'    import requests')
        code.append(f'    ')
        code.append(f'    url = "{url}"')
        code.append(f'    ')
        code.append(f'    try:')
        code.append(f'        response = requests.get(url, timeout=30)')
        code.append(f'        response.raise_for_status()')
        code.append(f'        ')
        code.append(f'        # Create FlowFile with fetched content')
        code.append(f'        output_ff = FlowFile(')
        code.append(f'            content=response.content,')
        code.append(f'            attributes={{')
        code.append(f'                "filename": "{filename}",')
        code.append(f'                "http.status.code": str(response.status_code),')
        code.append(f'                "mime.type": response.headers.get("Content-Type", "application/octet-stream")')
        code.append(f'            }}')
        code.append(f'        )')
        code.append(f'        return {{"success": [output_ff]}}')
        code.append(f'    except Exception as e:')
        code.append(f'        # On failure, pass original flowfile to failure relationship')
        code.append(f'        flowfile.attributes["error.message"] = str(e)')
        code.append(f'        return {{"failure": [flowfile]}}')

        return '\n'.join(code)


class UpdateAttributeConverter(ProcessorConverter):
    """Converter for UpdateAttribute processor"""

    def generate_function(self) -> str:
        # Get properties that are attribute updates
        # Filter out built-in properties
        builtin_props = {
            'Delete Attributes Expression', 'Store State', 'Stateful Variables Initial Value',
            'canonical-value-lookup-cache-size'
        }

        attribute_updates = {
            k: v for k, v in self.properties.items()
            if k not in builtin_props and v
        }

        delete_expression = self.properties.get('Delete Attributes Expression', '')

        code = []
        code.append(f"def process_updateattribute_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    UpdateAttribute: {self.processor_name}')
        code.append(f'    ')
        code.append(f'    Updates FlowFile attributes using Expression Language')
        code.append(f'    """')

        # Generate attribute updates
        if attribute_updates:
            code.append(f'    # Update attributes')
            for attr_name, el_expression in attribute_updates.items():
                # Transpile the EL expression
                python_expr = self.el_transpiler.transpile(el_expression)
                code.append(f'    flowfile.attributes["{attr_name}"] = {python_expr}')

        # Handle attribute deletion
        if delete_expression:
            code.append(f'    ')
            code.append(f'    # Delete attributes matching pattern')
            code.append(f'    delete_pattern = r"{delete_expression}"')
            code.append(f'    attrs_to_delete = [k for k in flowfile.attributes.keys() if re.match(delete_pattern, k)]')
            code.append(f'    for attr in attrs_to_delete:')
            code.append(f'        del flowfile.attributes[attr]')

        code.append(f'    ')
        code.append(f'    return {{"success": [flowfile]}}')

        return '\n'.join(code)


class RouteOnAttributeConverter(ProcessorConverter):
    """Converter for RouteOnAttribute processor"""

    def generate_function(self) -> str:
        # Get routing strategy
        routing_strategy = self.properties.get('Routing Strategy', 'Route to Property name')

        # Get routing rules (all properties except built-in ones)
        builtin_props = {
            'Routing Strategy'
        }

        routing_rules = {
            k: v for k, v in self.properties.items()
            if k not in builtin_props and v
        }

        code = []
        code.append(f"def process_routeonattribute_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    RouteOnAttribute: {self.processor_name}')
        code.append(f'    ')
        code.append(f'    Routes FlowFiles based on attribute conditions')
        code.append(f'    Routing Strategy: {routing_strategy}')
        code.append(f'    """')

        if not routing_rules:
            # No rules - route to unmatched
            code.append(f'    # No routing rules configured')
            code.append(f'    return {{"unmatched": [flowfile]}}')
        else:
            # Generate switch-style routing
            code.append(f'    # Check routing conditions')

            for rule_name, condition_expr in routing_rules.items():
                # Transpile boolean expression
                python_condition = self.el_transpiler.transpile_boolean_expression(condition_expr)

                code.append(f'    if {python_condition}:')
                code.append(f'        return {{"{rule_name}": [flowfile]}}')

            code.append(f'    ')
            code.append(f'    # No matches - route to unmatched')
            code.append(f'    return {{"unmatched": [flowfile]}}')

        return '\n'.join(code)


class GenerateFlowFileConverter(ProcessorConverter):
    """Converter for GenerateFlowFile processor"""

    def generate_function(self) -> str:
        # Get configuration
        custom_text = self.properties.get('Custom Text', '')
        file_size = self.properties.get('File Size', '0')
        batch_size = self.properties.get('Batch Size', '1')
        unique_flowfiles = self.properties.get('Unique FlowFiles', 'false').lower() == 'true'

        code = []
        code.append(f"def process_generateflowfile_{self.processor_id.replace('-', '_')[:16]}() -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    GenerateFlowFile: {self.processor_name}')
        code.append(f'    ')
        code.append(f'    Generates FlowFiles with custom content')
        code.append(f'    """')
        code.append(f'    flowfiles = []')
        code.append(f'    ')

        # Determine batch size
        if batch_size != '1':
            code.append(f'    batch_size = {batch_size}')
            code.append(f'    for i in range(batch_size):')
            indent = '    '
        else:
            indent = ''

        # Generate content
        if custom_text:
            # Use custom text
            custom_text_escaped = custom_text.replace('\\', '\\\\').replace('"', '\\"')
            code.append(f'{indent}    content = """{custom_text_escaped}"""')
        else:
            # Generate random data of specified size
            code.append(f'{indent}    # Generate random content of {file_size} bytes')
            code.append(f'{indent}    import random, string')
            code.append(f'{indent}    content = "".join(random.choices(string.ascii_letters, k={file_size}))')

        code.append(f'{indent}    ')
        code.append(f'{indent}    # Create FlowFile')
        code.append(f'{indent}    ff = FlowFile(')
        code.append(f'{indent}        content=content.encode("utf-8") if isinstance(content, str) else content,')
        code.append(f'{indent}        attributes={{"filename": f"generated_{{{{"i" if batch_size != "1" else "1"}}}}.txt"}}')
        code.append(f'{indent}    )')
        code.append(f'{indent}    flowfiles.append(ff)')

        code.append(f'    ')
        code.append(f'    return {{"success": flowfiles}}')

        return '\n'.join(code)


class ExecuteStreamCommandStubConverter(ProcessorConverter):
    """
    Generates smart stub for ExecuteStreamCommand

    Extracts command configuration and shows where data flows
    """

    def generate_function(self) -> str:
        # Extract command configuration
        command_path = self.properties.get('Command Path', '')
        command_args = self.properties.get('Command Arguments', '')
        working_dir = self.properties.get('Working Directory', '')

        # Determine if this returns data
        # Check if it has output stream relationship
        has_output = False
        output_relationship = None

        # Try to infer from provenance events
        for event in self.events:
            if event.get('componentId') == self.processor_id:
                relationship = event.get('relationship', '')
                if relationship in ('output stream', 'success'):
                    has_output = True
                    output_relationship = relationship
                    break

        code = []
        code.append(f"def process_executestreamcommand_{self.processor_id.replace('-', '_')[:16]}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
        code.append(f'    """')
        code.append(f'    ExecuteStreamCommand: {self.processor_name}')
        code.append(f'    ')
        code.append(f'    Configuration:')
        code.append(f'      Command Path: {command_path}')
        if command_args:
            code.append(f'      Command Arguments: {command_args}')
        if working_dir:
            code.append(f'      Working Directory: {working_dir}')
        code.append(f'    ')

        # Identify command type and suggest migration
        migration_hint = self._get_migration_hint(command_path, command_args)
        code.append(f'    Migration Suggestion:')
        code.append(f'      {migration_hint}')
        code.append(f'    ')

        if has_output:
            code.append(f'    Data Flow:')
            code.append(f'      This command produces output data ({output_relationship} relationship)')
            code.append(f'      The next processor in the flow processes this output')
            code.append(f'    ')

        code.append(f'    TODO: Implement command execution or migrate to native Python/SQL')
        code.append(f'    """')
        code.append(f'    ')
        code.append(f'    # Extract command arguments from flowfile attributes')

        if command_args and '${' in command_args:
            code.append(f'    # Command uses attributes: {command_args}')
            # Extract attribute references
            import re as regex_module
            attrs = regex_module.findall(r'\$\{([^}]+)\}', command_args)
            for attr in attrs:
                code.append(f'    # - {attr}: {{flowfile.attributes.get("{attr}", "")}}')

        code.append(f'    ')

        if has_output:
            code.append(f'    # TODO: Execute command and capture output')
            code.append(f'    # command_output = subprocess.run(["{command_path}", ...], capture_output=True)')
            code.append(f'    # ')
            code.append(f'    # output_ff = FlowFile(')
            code.append(f'    #     content=command_output.stdout,')
            code.append(f'    #     attributes=flowfile.attributes.copy()')
            code.append(f'    # )')
            code.append(f'    # return {{"{output_relationship}": [output_ff], "original": [flowfile]}}')
        else:
            code.append(f'    # TODO: Execute command (no output captured)')
            code.append(f'    # subprocess.run(["{command_path}", ...], check=True)')
            code.append(f'    # return {{"success": [flowfile]}}')

        code.append(f'    ')
        code.append(f'    raise NotImplementedError(')
        code.append(f'        "ExecuteStreamCommand requires manual implementation. "')
        code.append(f'        "See configuration and migration suggestions above."')
        code.append(f'    )')

        return '\n'.join(code)

    def _get_migration_hint(self, command_path: str, command_args: str) -> str:
        """Generate migration hint based on command"""
        command_lower = f"{command_path} {command_args}".lower()

        if 'impala-shell' in command_lower or 'impala' in command_lower:
            if 'refresh' in command_lower:
                return "Migrate to: Databricks SQL - REFRESH TABLE or OPTIMIZE"
            elif 'insert' in command_lower:
                return "Migrate to: Databricks SQL - INSERT or MERGE INTO"
            else:
                return "Migrate to: Databricks SQL - Replace Impala queries with Spark SQL"

        elif 'hdfs' in command_lower or 'hadoop fs' in command_lower:
            return "Migrate to: Python - Use dbutils.fs for DBFS operations"

        elif 'kinit' in command_lower or 'kerberos' in command_lower:
            return "Migrate to: Use cloud-native authentication (IAM, Service Principal)"

        elif any(cmd in command_lower for cmd in ['awk', 'sed', 'grep', 'cut']):
            return "Migrate to: Python - Use pandas, re, or native string operations"

        else:
            return "Migrate to: Python - Reimplement command logic in Python"


class FlowControlStubConverter(ProcessorConverter):
    """
    Generates documentation for flow control processors

    These processors don't transform data - they control flow timing/coordination
    """

    def generate_function(self) -> str:
        processor_type = self.component['type'].split('.')[-1]

        code = []
        code.append(f'# {processor_type}: {self.processor_name}')
        code.append(f'# ')
        code.append(f'# This processor is for flow control and does not transform data.')
        code.append(f'# It has been omitted from the generated Python code.')
        code.append(f'# ')

        if processor_type == 'Wait':
            code.append(f'# Wait processor: Pauses FlowFiles until a signal is received')
            code.append(f'# Release Signal ID: {self.properties.get("Release Signal Identifier", "N/A")}')
            code.append(f'# Python equivalent: Use shared cache or Redis for coordination')

        elif processor_type == 'Notify':
            code.append(f'# Notify processor: Sends signals to waiting FlowFiles')
            code.append(f'# Release Signal ID: {self.properties.get("Release Signal Identifier", "N/A")}')
            code.append(f'# Python equivalent: Update shared cache or Redis')

        elif processor_type == 'ControlRate':
            code.append(f'# ControlRate processor: Throttles FlowFile throughput')
            code.append(f'# Rate: {self.properties.get("Rate Control Criteria", "N/A")}')
            code.append(f'# Python equivalent: Often not needed in batch processing')

        elif processor_type == 'MonitorActivity':
            code.append(f'# MonitorActivity processor: Monitors flow activity')
            code.append(f'# Python equivalent: Use logging or monitoring framework')

        code.append(f'')

        return '\n'.join(code)


def get_converter(processor_config: Dict[str, Any], provenance_events: List[Dict[str, Any]]) -> Optional[ProcessorConverter]:
    """Get the appropriate converter for a processor type"""
    processor_type = processor_config['component']['type'].split('.')[-1]

    # Full implementations
    converters = {
        'DetectDuplicate': DetectDuplicateConverter,
        'ExtractText': ExtractTextConverter,
        'RouteText': RouteTextConverter,
        'SplitText': SplitTextConverter,
        'GetHTTP': GetHTTPConverter,
        'UpdateAttribute': UpdateAttributeConverter,
        'RouteOnAttribute': RouteOnAttributeConverter,
        'GenerateFlowFile': GenerateFlowFileConverter,
    }

    # Special stub converters
    stub_converters = {
        'ExecuteStreamCommand': ExecuteStreamCommandStubConverter,
    }

    # Flow control processors (documentation only)
    flow_control_processors = {
        'Wait', 'Notify', 'ControlRate', 'MonitorActivity'
    }

    # Check for full implementation
    converter_class = converters.get(processor_type)
    if converter_class:
        return converter_class(processor_config, provenance_events)

    # Check for stub converter
    stub_class = stub_converters.get(processor_type)
    if stub_class:
        return stub_class(processor_config, provenance_events)

    # Check for flow control (documentation only)
    if processor_type in flow_control_processors:
        return FlowControlStubConverter(processor_config, provenance_events)

    # Unknown processor type
    return None
