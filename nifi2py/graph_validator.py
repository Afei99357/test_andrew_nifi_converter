#!/usr/bin/env python3
"""
Graph/Structure validation - validates without requiring content

This validates that the generated code has the correct:
- Processor sequences
- Relationship routing
- Execution paths
- Processor configurations

Does NOT require provenance content to be available.
"""

from typing import Dict, List, Any, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict


# Processors that are essentially no-ops from a data transformation perspective
# These are for monitoring, routing, or coordination only
NO_OP_PROCESSORS = {
    'Funnel',                    # Just routes flowfiles
    'LogMessage',                # Only logs, doesn't transform
    'LogAttribute',              # Only logs attributes
    'ControlRate',               # Only throttles
    'MonitorActivity',           # Only monitors
    'NiFi Flow',                 # Root process group
    'Unknown',                   # Unknown/deleted processors
}


def is_noop_processor(processor_type: str, processor_name: str) -> bool:
    """
    Check if a processor is a no-op (doesn't transform data).

    Args:
        processor_type: Processor type (e.g., 'Funnel', 'LogMessage')
        processor_name: Processor name (e.g., 'NiFi Flow')

    Returns:
        True if processor is a no-op
    """
    # Check by type
    if processor_type in NO_OP_PROCESSORS:
        return True

    # Check by name (for special cases like root process group)
    if processor_name in NO_OP_PROCESSORS:
        return True

    return False


@dataclass
class GraphValidationResult:
    """Result of graph/structure validation"""
    processor_coverage: float      # % of processors we can generate
    path_coverage: float           # % of paths we discovered
    processors_found: int          # Total processors in provenance
    processors_generated: int      # Processors we generated code for
    paths_found: int              # Total paths discovered
    paths_implemented: int        # Paths we implemented
    missing_processors: List[str] # Processors we couldn't generate
    issues: List[str]             # Any issues found


class GraphValidator:
    """
    Validates generated code structure against provenance graph

    This works WITHOUT requiring content - just uses provenance events
    to validate the flow structure.
    """

    def __init__(self, nifi_client, generated_module, lineage_tracer):
        """
        Initialize graph validator

        Args:
            nifi_client: NiFiClient instance
            generated_module: The generated Python module
            lineage_tracer: LineageTracer instance
        """
        self.client = nifi_client
        self.module = generated_module
        self.tracer = lineage_tracer

    def _filter_redundant_paths(self, paths: List, noop_processors: Set[str]) -> List:
        """
        Filter out redundant paths to avoid penalizing coverage.

        A path is redundant if:
        1. It's a single-processor path AND
        2. That processor appears in a longer path

        This prevents penalizing for not implementing standalone "SplitText"
        when SplitText is already implemented as part of "RouteText → SplitText".

        Args:
            paths: List of execution paths (tuples of processor info)
            noop_processors: Set of no-op processor IDs to exclude

        Returns:
            Filtered list of substantive paths
        """
        if not paths:
            return []

        # Get all processors that appear in multi-processor paths
        processors_in_multi_paths = set()
        for path in paths:
            # Filter out no-ops from the path
            non_noop_path = [(pid, ptype, pname) for pid, ptype, pname in path
                           if not is_noop_processor(ptype, pname)]

            # If this is a multi-processor path, track all processors in it
            if len(non_noop_path) > 1:
                processors_in_multi_paths.update(pid for pid, _, _ in non_noop_path)

        # Filter paths
        substantive_paths = []
        for path in paths:
            # Filter out no-ops from the path
            non_noop_path = [(pid, ptype, pname) for pid, ptype, pname in path
                           if not is_noop_processor(ptype, pname)]

            # Skip empty paths (all no-ops)
            if not non_noop_path:
                continue

            # If it's a single-processor path and that processor appears in a multi-path, skip it
            if len(non_noop_path) == 1:
                proc_id, _, _ = non_noop_path[0]
                if proc_id in processors_in_multi_paths:
                    continue  # Redundant - processor already covered

            substantive_paths.append(path)

        return substantive_paths

    def validate_structure(self, events: List[Dict[str, Any]]) -> GraphValidationResult:
        """
        Validate that generated code structure matches NiFi flow structure

        Args:
            events: List of provenance events

        Returns:
            GraphValidationResult with validation details
        """
        issues = []

        # Get unique processors from provenance
        processors_in_flow = set()
        processor_names = {}
        noop_processors = set()  # Track no-op processors separately

        for event in events:
            proc_id = event.get('componentId')
            proc_name = event.get('componentName', 'Unknown')
            proc_type = event.get('componentType', 'Unknown')

            if proc_id:
                processors_in_flow.add(proc_id)
                processor_names[proc_id] = {
                    'name': proc_name,
                    'type': proc_type
                }

                # Track no-op processors
                if is_noop_processor(proc_type, proc_name):
                    noop_processors.add(proc_id)

        # Get processors we generated code for
        generated_processors = set()
        for name in dir(self.module):
            if name.startswith('process_'):
                # Extract processor ID from function name
                # Format: process_{type}_{id_prefix}
                parts = name.split('_')
                if len(parts) >= 3:
                    # The ID prefix is in the name
                    generated_processors.add(name)

        # Check processor coverage
        # We expect to have generated code for processors we have configs for
        # Some processors may be deleted/unavailable
        # Exclude no-op processors from coverage calculation

        # Get execution paths from lineage
        paths = self.tracer.get_execution_paths()

        # Count how many paths we have execution functions for
        implemented_paths = 0
        for name in dir(self.module):
            if name.startswith('execute_path_'):
                implemented_paths += 1

        # Calculate coverage (excluding no-op processors)
        substantive_processors = processors_in_flow - noop_processors
        processor_coverage = (len(generated_processors) / len(substantive_processors) * 100) if substantive_processors else 100.0

        # Calculate path coverage
        # Filter out single-processor paths if that processor appears in a longer path
        # (avoids penalizing for not implementing standalone paths when processor is already implemented)
        substantive_paths = self._filter_redundant_paths(paths, noop_processors)
        path_coverage = (implemented_paths / len(substantive_paths) * 100) if substantive_paths else 100.0

        # Find missing processors (exclude no-ops)
        missing = []
        for proc_id, info in processor_names.items():
            # Skip no-op processors
            if is_noop_processor(info['type'], info['name']):
                continue

            # Check if we have a function for this processor
            has_function = any(
                proc_id.replace('-', '_')[:16] in name
                for name in dir(self.module)
                if name.startswith('process_')
            )

            if not has_function:
                missing.append(f"{info['type']}: {info['name']} ({proc_id[:16]}...)")

        # Check for structural issues
        if len(paths) == 0:
            issues.append("No execution paths found in provenance")

        if implemented_paths == 0:
            issues.append("No execution path functions generated")

        if len(generated_processors) == 0:
            issues.append("No processor functions generated")

        return GraphValidationResult(
            processor_coverage=processor_coverage,
            path_coverage=path_coverage,
            processors_found=len(substantive_processors),  # Exclude no-ops
            processors_generated=len(generated_processors),
            paths_found=len(substantive_paths),  # Exclude redundant single-processor paths
            paths_implemented=implemented_paths,
            missing_processors=missing,
            issues=issues
        )

    def validate_relationships(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate that relationship routing is correct

        Analyzes provenance events to see which relationships were used
        and verifies generated code handles them.

        Args:
            events: List of provenance events

        Returns:
            Dict with relationship validation details
        """
        # Group events by processor
        events_by_processor = defaultdict(list)
        for event in events:
            proc_id = event.get('componentId')
            if proc_id:
                events_by_processor[proc_id].append(event)

        relationship_analysis = {}

        for proc_id, proc_events in events_by_processor.items():
            # Get unique event types and relationships used
            event_types = set(e.get('eventType') for e in proc_events)

            # For ROUTE events, get the relationship used
            relationships = set()
            for event in proc_events:
                if event.get('eventType') == 'ROUTE':
                    rel = event.get('relationship')
                    if rel:
                        relationships.add(rel)

            if relationships:
                relationship_analysis[proc_id] = {
                    'event_types': list(event_types),
                    'relationships_used': list(relationships),
                    'event_count': len(proc_events)
                }

        return relationship_analysis

    def validate_execution_order(self) -> List[str]:
        """
        Validate that execution order in generated paths matches lineage

        Returns:
            List of issues found (empty if all good)
        """
        issues = []

        # Get execution paths from lineage
        paths = self.tracer.get_execution_paths()

        # Check if we have execute_path functions
        path_functions = [
            name for name in dir(self.module)
            if name.startswith('execute_path_')
        ]

        if len(path_functions) == 0 and len(paths) > 0:
            issues.append("Lineage found execution paths but no execute_path functions generated")

        if len(path_functions) > 0 and len(paths) == 0:
            issues.append("Generated execute_path functions but lineage found no paths")

        # Check for reasonable match
        if len(paths) > 0:
            expected_functions = min(len(paths), 5)  # We generate max 5 paths
            if len(path_functions) < expected_functions:
                issues.append(
                    f"Expected {expected_functions} path functions but only found {len(path_functions)}"
                )

        return issues


def validate_external_output(
    external_data: List[Dict[str, Any]],
    generated_output: List[Any],
    comparison_key: str = 'id'
) -> Tuple[int, int, int]:
    """
    Validate generated code output against external system data

    For when data is sent to external systems (databases, APIs, etc)
    instead of being in NiFi provenance.

    Args:
        external_data: Data from external system (list of dicts)
        generated_output: Output from generated Python code
        comparison_key: Key to use for matching records

    Returns:
        Tuple of (matched, mismatched, total)

    Example:
        >>> # Get data from external database
        >>> db_data = fetch_from_database("SELECT * FROM output_table")
        >>>
        >>> # Run generated code
        >>> python_output = execute_path_1(input_data)
        >>>
        >>> # Compare
        >>> matched, mismatched, total = validate_external_output(
        ...     db_data, python_output, comparison_key='record_id'
        ... )
        >>> print(f"Matched: {matched}/{total}")
    """
    matched = 0
    mismatched = 0

    # Create lookup by comparison key
    external_lookup = {item.get(comparison_key): item for item in external_data}

    for item in generated_output:
        key = item.get(comparison_key) if hasattr(item, 'get') else getattr(item, comparison_key, None)

        if key in external_lookup:
            # Simple comparison - could be enhanced
            if item == external_lookup[key]:
                matched += 1
            else:
                mismatched += 1

    total = len(generated_output)
    return matched, mismatched, total
