"""
Generate Python code from NiFi provenance data.

This is the CORRECT approach - we generate code based on actual execution,
not just template structure.

The provenance-driven workflow:
1. Query provenance events for a processor
2. Fetch processor configuration via REST API
3. Analyze observed behavior patterns from provenance
4. Extract and transpile EL expressions from config
5. Generate Python function that replicates observed behavior
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path
from collections import defaultdict
import logging
import re

from nifi2py.client import NiFiClient
from nifi2py.expression_language import ELTranspiler
from nifi2py.provenance_extractor import ProvenanceExtractor, ProcessorExecution, ExecutionSample

logger = logging.getLogger(__name__)


@dataclass
class ProvenanceSnapshot:
    """A snapshot of processor behavior from provenance + REST API config"""
    processor_id: str
    processor_name: str
    processor_type: str

    # Configuration from REST API
    properties: Dict[str, str]
    relationships: List[str]

    # Observed behavior from provenance
    processor_execution: ProcessorExecution

    @property
    def execution_samples(self) -> List[ExecutionSample]:
        """Get execution samples from processor execution"""
        return self.processor_execution.executions

    @property
    def has_samples(self) -> bool:
        """Check if we have provenance samples"""
        return len(self.execution_samples) > 0


class ProvenanceDrivenGenerator:
    """Generate Python code from provenance + processor configs"""

    def __init__(self, client: NiFiClient):
        """
        Initialize provenance-driven generator.

        Args:
            client: Authenticated NiFi client
        """
        self.client = client
        self.el_transpiler = ELTranspiler()
        self.provenance_extractor = ProvenanceExtractor(client)

    def collect_provenance_snapshot(
        self,
        processor_id: str,
        sample_size: int = 20
    ) -> ProvenanceSnapshot:
        """
        Collect provenance data + config for a processor.

        This is the key method - it walks provenance and fetches config.

        Args:
            processor_id: Processor to analyze
            sample_size: Number of provenance samples to collect

        Returns:
            ProvenanceSnapshot containing config and observed behavior
        """
        logger.info(f"Collecting provenance snapshot for processor {processor_id}")

        # Step 1: Fetch processor configuration via REST API
        processor_data = self.client.get_processor(processor_id)
        component = processor_data['component']

        # Step 2: Extract execution samples from provenance
        processor_execution = self.provenance_extractor.extract_processor_executions(
            processor_id=processor_id,
            sample_size=sample_size
        )

        # Step 3: Extract relationships
        relationships = []
        for rel in processor_data.get('relationships', []):
            relationships.append(rel['name'])

        return ProvenanceSnapshot(
            processor_id=processor_id,
            processor_name=component['name'],
            processor_type=component['type'],
            properties=component['config']['properties'],
            relationships=relationships,
            processor_execution=processor_execution
        )

    def analyze_patterns(self, samples: List[ExecutionSample]) -> Dict:
        """
        Analyze patterns across execution samples.

        Args:
            samples: List of execution samples from provenance

        Returns:
            Dict containing pattern analysis
        """
        if not samples:
            return {
                'always_added': {},
                'always_modified': [],
                'content_changed': False,
                'routing': {},
                'attribute_values': {}
            }

        # Find attributes added in ALL samples
        added_attrs = defaultdict(set)
        for sample in samples:
            for attr, value in sample.attributes_added.items():
                added_attrs[attr].add(value)

        # Attributes with constant value across all samples
        always_added = {
            attr: list(values)[0]
            for attr, values in added_attrs.items()
            if len(values) == 1 and len(samples) > 1
        }

        # Attributes that change value (modified or added with different values)
        modified_attrs = set()
        for sample in samples:
            modified_attrs.update(sample.attributes_modified.keys())

        # Add attributes with varying values
        for attr, values in added_attrs.items():
            if len(values) > 1:
                modified_attrs.add(attr)

        # Content changes
        content_changed = any(s.content_changed for s in samples)

        # Routing patterns (not directly in ExecutionSample, infer from relationships)
        # For now, just return empty dict as routing is in connections
        routing = {}

        # Collect sample attribute values for reference
        attribute_values = {}
        for sample in samples:
            for attr, value in sample.output_attributes.items():
                if attr not in attribute_values:
                    attribute_values[attr] = []
                if value not in attribute_values[attr]:
                    attribute_values[attr].append(value)

        return {
            'always_added': always_added,
            'always_modified': list(modified_attrs),
            'content_changed': content_changed,
            'routing': routing,
            'attribute_values': attribute_values
        }

    def generate_python_function(self, snapshot: ProvenanceSnapshot) -> str:
        """
        Generate Python function from provenance snapshot.

        This combines:
        1. Observed behavior (from provenance)
        2. Configuration (from REST API)
        3. Transpiled EL expressions

        Args:
            snapshot: ProvenanceSnapshot containing config and behavior

        Returns:
            Python function code as string
        """
        proc_type = snapshot.processor_type.split('.')[-1]
        func_name = f"process_{proc_type.lower()}_{snapshot.processor_id[:8]}"

        # Analyze patterns from provenance
        patterns = self.analyze_patterns(snapshot.execution_samples)

        # Generate based on processor type
        if proc_type == "UpdateAttribute":
            return self._generate_update_attribute(snapshot, func_name, patterns)
        elif proc_type == "RouteOnAttribute":
            return self._generate_route_on_attribute(snapshot, func_name, patterns)
        elif proc_type == "LogMessage" or proc_type == "LogAttribute":
            return self._generate_log_message(snapshot, func_name, patterns)
        elif proc_type == "GenerateFlowFile":
            return self._generate_generate_flowfile(snapshot, func_name, patterns)
        elif proc_type == "ReplaceText":
            return self._generate_replace_text(snapshot, func_name, patterns)
        elif proc_type == "ExecuteStreamCommand":
            return self._generate_execute_stream_command(snapshot, func_name, patterns)
        else:
            return self._generate_stub(snapshot, func_name, patterns)

    def _generate_update_attribute(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate UpdateAttribute function from provenance + config"""

        lines = [
            f"def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:",
            f'    """',
            f'    Generated from NiFi UpdateAttribute processor',
            f'    Original Processor ID: {snapshot.processor_id}',
            f'    Name: {snapshot.processor_name}',
            f'    ',
        ]

        # Document observed patterns
        if snapshot.has_samples:
            lines.append(f'    Observed behavior from {len(snapshot.execution_samples)} provenance samples:')
            if patterns['always_added']:
                lines.append('    Attributes always added:')
                for attr, value in patterns['always_added'].items():
                    lines.append(f'      {attr} = {value}')
            if patterns['always_modified']:
                lines.append('    Attributes sometimes modified:')
                for attr in patterns['always_modified']:
                    sample_vals = patterns['attribute_values'].get(attr, [])
                    vals_str = ', '.join(str(v)[:30] for v in sample_vals[:3])
                    lines.append(f'      {attr} (e.g., {vals_str})')

        lines.append('    """')

        # Get attributes from processor config
        has_rules = False
        for key, value in snapshot.properties.items():
            if key and value:
                has_rules = True
                # Transpile EL expression
                try:
                    if '${' in value:
                        python_expr = self.el_transpiler.transpile_embedded(value)
                        lines.append(f"    flowfile.attributes['{key}'] = {python_expr}")
                    else:
                        # Literal value
                        lines.append(f"    flowfile.attributes['{key}'] = '{value}'")
                except Exception as e:
                    logger.warning(f"Failed to transpile '{value}': {e}")
                    # Fallback to literal
                    lines.append(f"    flowfile.attributes['{key}'] = '{value}'")

        if not has_rules:
            lines.append("    # No attribute rules configured")

        lines.append("    return {'success': [flowfile]}")

        return '\n'.join(lines)

    def _generate_route_on_attribute(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate RouteOnAttribute from provenance + config"""

        lines = [
            f"def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:",
            f'    """',
            f'    Generated from NiFi RouteOnAttribute processor',
            f'    Original Processor ID: {snapshot.processor_id}',
            f'    Name: {snapshot.processor_name}',
        ]

        if snapshot.has_samples:
            lines.append(f'    Analyzed {len(snapshot.execution_samples)} provenance samples')

        lines.append('    """')

        # Generate routing logic from properties
        has_rules = False
        for key, value in snapshot.properties.items():
            if key == 'Routing Strategy':
                continue
            if value and '${' in value:
                has_rules = True
                try:
                    # RouteOnAttribute conditions should return boolean
                    condition = self.el_transpiler.transpile(value)
                    lines.append(f"    if {condition}:")
                    lines.append(f"        return {{'{key}': [flowfile]}}")
                except Exception as e:
                    logger.warning(f"Failed to transpile routing condition '{value}': {e}")
                    lines.append(f"    # TODO: Failed to transpile condition for '{key}': {value}")

        if not has_rules:
            lines.append("    # No routing rules configured")

        lines.append("    return {'unmatched': [flowfile]}")

        return '\n'.join(lines)

    def _generate_log_message(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate LogMessage/LogAttribute function"""
        log_msg = snapshot.properties.get('log-message',
                  snapshot.properties.get('Log message', 'Processing: ${filename}'))
        log_level = snapshot.properties.get('log-level',
                    snapshot.properties.get('Log Level', 'INFO'))

        try:
            if '${' in log_msg:
                python_msg = self.el_transpiler.transpile_embedded(log_msg)
            else:
                python_msg = f"'{log_msg}'"
        except Exception as e:
            logger.warning(f"Failed to transpile log message '{log_msg}': {e}")
            python_msg = f"'{log_msg}'"

        return f'''def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    """
    LogMessage: {snapshot.processor_name}
    Original Processor ID: {snapshot.processor_id}
    """
    logger.{log_level.lower()}({python_msg})
    return {{'success': [flowfile]}}'''

    def _generate_generate_flowfile(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate GenerateFlowFile function"""
        custom_text = snapshot.properties.get('Custom Text', '')
        file_size = snapshot.properties.get('File Size', '0 B')

        lines = [
            f"def {func_name}() -> Dict[str, List[FlowFile]]:",
            f'    """',
            f'    GenerateFlowFile: {snapshot.processor_name}',
            f'    Original Processor ID: {snapshot.processor_id}',
            f'    """',
        ]

        if custom_text:
            try:
                if '${' in custom_text:
                    content_expr = self.el_transpiler.transpile_embedded(custom_text)
                    lines.append(f"    content = {content_expr}.encode('utf-8')")
                else:
                    lines.append(f"    content = '''{custom_text}'''.encode('utf-8')")
            except:
                lines.append(f"    content = '''{custom_text}'''.encode('utf-8')")
        else:
            lines.append(f"    content = b''")

        lines.append("    flowfile = FlowFile(content=content, attributes={})")
        lines.append("    return {'success': [flowfile]}")

        return '\n'.join(lines)

    def _generate_replace_text(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate ReplaceText function"""
        search_value = snapshot.properties.get('Search Value', '')
        replacement_value = snapshot.properties.get('Replacement Value', '')

        lines = [
            f"def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:",
            f'    """',
            f'    ReplaceText: {snapshot.processor_name}',
            f'    Original Processor ID: {snapshot.processor_id}',
            f'    """',
            f"    # Search: {search_value}",
            f"    # Replace: {replacement_value}",
            f"    content = flowfile.content.decode('utf-8')",
        ]

        if search_value:
            # Use regex replacement
            lines.append(f"    content = re.sub(r'''{search_value}''', r'''{replacement_value}''', content)")

        lines.append("    flowfile.content = content.encode('utf-8')")
        lines.append("    return {'success': [flowfile]}")

        return '\n'.join(lines)

    def _generate_execute_stream_command(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate ExecuteStreamCommand stub (needs manual implementation)"""
        command_path = snapshot.properties.get('Command Path', '')
        command_args = snapshot.properties.get('Command Arguments', '')

        return f'''def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    """
    ExecuteStreamCommand: {snapshot.processor_name}
    Original Processor ID: {snapshot.processor_id}

    Command: {command_path}
    Args: {command_args}

    TODO: This processor requires manual implementation
    Consider alternatives:
    - If running Impala queries → Use Databricks SQL
    - If running shell scripts → Refactor to Python
    - If running data transformations → Use pandas/polars
    """
    raise NotImplementedError(
        "ExecuteStreamCommand requires manual migration. "
        "See function docstring for alternatives."
    )'''

    def _generate_stub(self, snapshot: ProvenanceSnapshot, func_name: str, patterns: Dict) -> str:
        """Generate stub for unsupported processor"""
        proc_type = snapshot.processor_type.split('.')[-1]

        lines = [
            f"def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:",
            f'    """',
            f'    TODO: Implement {proc_type}',
            f'    Processor: {snapshot.processor_name}',
            f'    ID: {snapshot.processor_id}',
            f'    Type: {snapshot.processor_type}',
            f'    ',
        ]

        if snapshot.has_samples:
            lines.append(f'    Observed {len(snapshot.execution_samples)} executions in provenance')
            if patterns['always_added']:
                lines.append('    Attributes added:')
                for attr in patterns['always_added']:
                    lines.append(f'      - {attr}')

        lines.extend([
            f'    """',
            f'    raise NotImplementedError("Processor type {proc_type} not yet supported")'
        ])

        return '\n'.join(lines)

    def generate_flow_module(
        self,
        processor_ids: List[str],
        sample_size: int = 10,
        output_path: Optional[Path] = None
    ) -> str:
        """
        Generate complete Python module from multiple processors.

        Args:
            processor_ids: List of processor IDs to generate code for
            sample_size: Number of provenance samples per processor
            output_path: Optional path to write generated code

        Returns:
            Generated Python module code
        """
        logger.info(f"Generating Python module for {len(processor_ids)} processors")

        # Collect snapshots
        snapshots = []
        for proc_id in processor_ids:
            try:
                snapshot = self.collect_provenance_snapshot(proc_id, sample_size)
                snapshots.append(snapshot)
                logger.info(f"Collected snapshot for {snapshot.processor_name}")
            except Exception as e:
                logger.error(f"Failed to collect snapshot for {proc_id}: {e}")

        # Generate header
        lines = [
            '"""',
            'Generated from NiFi provenance data',
            '',
            'This code was automatically generated by nifi2py using the provenance-driven approach:',
            '1. Queried NiFi provenance repository for execution samples',
            '2. Fetched processor configurations via REST API',
            '3. Analyzed observed behavior patterns',
            '4. Transpiled NiFi Expression Language to Python',
            '5. Generated Python functions replicating NiFi logic',
            '',
            f'Total processors: {len(snapshots)}',
            f'Provenance samples per processor: {sample_size}',
            '"""',
            '',
            'from typing import Dict, List',
            'from dataclasses import dataclass',
            'import logging',
            'import re',
            'import uuid',
            'from datetime import datetime',
            '',
            'logger = logging.getLogger(__name__)',
            '',
            '',
            '@dataclass',
            'class FlowFile:',
            '    """Represents a NiFi FlowFile"""',
            '    content: bytes',
            '    attributes: Dict[str, str]',
            '',
            '',
            '# Helper functions for NiFi Expression Language',
            'def _substring_before(text: str, delimiter: str) -> str:',
            '    """Return substring before first occurrence of delimiter."""',
            '    idx = text.find(delimiter)',
            '    return text[:idx] if idx >= 0 else text',
            '',
            '',
            'def _substring_after(text: str, delimiter: str) -> str:',
            '    """Return substring after first occurrence of delimiter."""',
            '    idx = text.find(delimiter)',
            '    return text[idx + len(delimiter):] if idx >= 0 else text',
            '',
            '',
        ]

        # Generate functions
        for snapshot in snapshots:
            func_code = self.generate_python_function(snapshot)
            lines.append('')
            lines.append(func_code)
            lines.append('')

        # Generate processor mapping
        lines.extend([
            '',
            '# Processor ID to function mapping',
            'PROCESSOR_FUNCTIONS = {',
        ])

        for snapshot in snapshots:
            proc_type = snapshot.processor_type.split('.')[-1]
            func_name = f"process_{proc_type.lower()}_{snapshot.processor_id[:8]}"
            lines.append(f"    '{snapshot.processor_id}': {func_name},  # {snapshot.processor_name}")

        lines.extend([
            '}',
            '',
        ])

        module_code = '\n'.join(lines)

        # Write to file if specified
        if output_path:
            output_path.write_text(module_code)
            logger.info(f"Wrote generated module to {output_path}")

        return module_code
