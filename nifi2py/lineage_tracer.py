#!/usr/bin/env python3
"""
FlowFile Lineage Tracer - Follow FlowFiles through the provenance graph
"""

from typing import Dict, List, Any, Set, Tuple
from collections import defaultdict
import networkx as nx


class LineageTracer:
    """Trace FlowFile lineage through provenance events"""

    def __init__(self, provenance_events: List[Dict[str, Any]]):
        self.events = provenance_events
        self.events_by_uuid = self._index_events()
        self.graph = self._build_lineage_graph()

    def _index_events(self) -> Dict[str, List[Dict[str, Any]]]:
        """Index events by FlowFile UUID for faster lookup"""
        indexed = defaultdict(list)
        for event in self.events:
            uuid = event.get('flowFileUuid')
            if uuid:
                indexed[uuid].append(event)

        # Sort events for each UUID by time
        for uuid in indexed:
            indexed[uuid].sort(key=lambda e: e.get('eventTime', ''))

        return indexed

    def _build_lineage_graph(self) -> nx.DiGraph:
        """Build a directed graph of FlowFile lineage"""
        graph = nx.DiGraph()

        for event in self.events:
            flowfile_uuid = event.get('flowFileUuid')
            component_id = event.get('componentId')
            event_type = event.get('eventType')
            component_name = event.get('componentName', 'Unknown')
            component_type = event.get('componentType', 'Unknown')

            # Add node for this FlowFile
            if not graph.has_node(flowfile_uuid):
                graph.add_node(flowfile_uuid, events=[])

            # Add event to node data
            graph.nodes[flowfile_uuid]['events'].append(event)

            # Add edges for parent relationships
            parent_uuids = event.get('parentUuids', [])
            for parent_uuid in parent_uuids:
                if parent_uuid != flowfile_uuid:  # Skip self-references
                    graph.add_edge(
                        parent_uuid,
                        flowfile_uuid,
                        event_type=event_type,
                        component_id=component_id,
                        component_name=component_name,
                        component_type=component_type
                    )

            # Add edges for child relationships
            child_uuids = event.get('childUuids', [])
            for child_uuid in child_uuids:
                if child_uuid != flowfile_uuid:  # Skip self-references
                    graph.add_edge(
                        flowfile_uuid,
                        child_uuid,
                        event_type=event_type,
                        component_id=component_id,
                        component_name=component_name,
                        component_type=component_type
                    )

        return graph

    def get_journey(self, flowfile_uuid: str) -> List[Dict[str, Any]]:
        """Get all provenance events for a specific FlowFile"""
        return self.events_by_uuid.get(flowfile_uuid, [])

    def get_ancestors(self, flowfile_uuid: str) -> List[str]:
        """Get all ancestor FlowFile UUIDs (parents, grandparents, etc.)"""
        if flowfile_uuid not in self.graph:
            return []

        try:
            # Find all nodes that can reach this one
            ancestors = nx.ancestors(self.graph, flowfile_uuid)
            return list(ancestors)
        except nx.NetworkXError:
            return []

    def get_descendants(self, flowfile_uuid: str) -> List[str]:
        """Get all descendant FlowFile UUIDs (children, grandchildren, etc.)"""
        if flowfile_uuid not in self.graph:
            return []

        try:
            # Find all nodes reachable from this one
            descendants = nx.descendants(self.graph, flowfile_uuid)
            return list(descendants)
        except nx.NetworkXError:
            return []

    def get_root_flowfiles(self) -> List[str]:
        """Get FlowFiles that have no parents (ingress points)"""
        roots = []
        for node in self.graph.nodes():
            # A root has no incoming edges from other nodes
            predecessors = list(self.graph.predecessors(node))
            if not predecessors:
                roots.append(node)

        return roots

    def get_leaf_flowfiles(self) -> List[str]:
        """Get FlowFiles that have no children (egress points or drops)"""
        leaves = []
        for node in self.graph.nodes():
            # A leaf has no outgoing edges to other nodes
            successors = list(self.graph.successors(node))
            if not successors:
                leaves.append(node)

        return leaves

    def trace_lineage(self, flowfile_uuid: str) -> List[Tuple[str, List[Dict[str, Any]]]]:
        """
        Trace complete lineage from ancestors to descendants.
        Returns list of (flowfile_uuid, events) tuples in chronological order.
        """
        if flowfile_uuid not in self.graph:
            return []

        # Get all related FlowFiles (ancestors + self + descendants)
        ancestors = self.get_ancestors(flowfile_uuid)
        descendants = self.get_descendants(flowfile_uuid)
        all_uuids = set(ancestors) | {flowfile_uuid} | set(descendants)

        # Build lineage list with events
        lineage = []
        for uuid in all_uuids:
            events = self.get_journey(uuid)
            if events:
                lineage.append((uuid, events))

        # Sort by earliest event time
        lineage.sort(key=lambda x: x[1][0].get('eventTime', '') if x[1] else '')

        return lineage

    def get_processor_sequence(self, flowfile_uuid: str) -> List[Tuple[str, str, str]]:
        """
        Get the sequence of processors a FlowFile passed through.
        Returns list of (processor_id, processor_type, processor_name) tuples.
        """
        events = self.get_journey(flowfile_uuid)

        # Track processors in order (avoiding duplicates while preserving order)
        seen_processors = set()
        processor_sequence = []

        for event in events:
            proc_id = event.get('componentId')
            proc_type = event.get('componentType', 'Unknown')
            proc_name = event.get('componentName', 'Unknown')

            if proc_id and proc_id not in seen_processors:
                seen_processors.add(proc_id)
                processor_sequence.append((proc_id, proc_type, proc_name))

        return processor_sequence

    def get_execution_paths(self) -> List[List[Tuple[str, str, str]]]:
        """
        Get all unique execution paths from ingress to egress.
        Each path is a list of (processor_id, processor_type, processor_name) tuples.
        """
        paths = []

        # Start from each root FlowFile
        for root_uuid in self.get_root_flowfiles():
            # Get the processor sequence for this root
            proc_sequence = self.get_processor_sequence(root_uuid)

            # Also trace descendants
            descendants = self.get_descendants(root_uuid)
            for desc_uuid in descendants:
                desc_sequence = self.get_processor_sequence(desc_uuid)
                if desc_sequence and desc_sequence not in paths:
                    paths.append(desc_sequence)

            if proc_sequence and proc_sequence not in paths:
                paths.append(proc_sequence)

        return paths

    def print_lineage(self, flowfile_uuid: str):
        """Pretty print the complete lineage for a FlowFile"""
        print(f"Lineage for FlowFile: {flowfile_uuid}")
        print("=" * 80)

        lineage = self.trace_lineage(flowfile_uuid)

        if not lineage:
            print("No lineage found")
            return

        for i, (uuid, events) in enumerate(lineage):
            print(f"\n{i + 1}. FlowFile: {uuid[:16]}...")
            for j, event in enumerate(events):
                event_type = event.get('eventType', 'UNKNOWN')
                component_name = event.get('componentName', 'Unknown')
                component_type = event.get('componentType', 'Unknown')
                event_time = event.get('eventTime', '')

                print(f"   {j + 1}. {event_type:15s} @ {component_type:20s} ({component_name})")
                print(f"      Time: {event_time}")

                # Show children if any
                children = event.get('childUuids', [])
                if children:
                    print(f"      → Created {len(children)} children")

    def print_execution_paths(self):
        """Pretty print all execution paths"""
        paths = self.get_execution_paths()

        print("Execution Paths:")
        print("=" * 80)

        for i, path in enumerate(paths):
            print(f"\nPath {i + 1}:")
            for j, (proc_id, proc_type, proc_name) in enumerate(path):
                arrow = " → " if j > 0 else "   "
                print(f"{arrow}{proc_type}: {proc_name}")
                print(f"      (ID: {proc_id[:16]}...)")
