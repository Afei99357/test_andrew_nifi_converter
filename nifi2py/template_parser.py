"""
NiFi Template Parser

Parses Apache NiFi flow template XML files and extracts:
- Processors with their configurations
- Connections between processors
- Expression Language (EL) expressions
- Flow statistics

"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from lxml import etree

# Temporary data models (will be moved to models.py later)


@dataclass
class Processor:
    """Represents a NiFi Processor"""

    id: str
    name: str
    type: str
    parent_group_id: str
    properties: Dict[str, Optional[str]] = field(default_factory=dict)
    relationships: Dict[str, bool] = field(default_factory=dict)  # name -> auto_terminate
    state: str = "STOPPED"
    scheduling_strategy: Optional[str] = None
    scheduling_period: Optional[str] = None
    position: Optional[Tuple[float, float]] = None
    comments: Optional[str] = None

    def get_short_type(self) -> str:
        """Get the short type name (e.g., 'UpdateAttribute' from full class name)"""
        return self.type.split(".")[-1] if self.type else "Unknown"


@dataclass
class Connection:
    """Represents a connection between processors"""

    id: str
    source_id: str
    source_type: str
    destination_id: str
    destination_type: str
    relationships: List[str] = field(default_factory=list)
    name: Optional[str] = None
    parent_group_id: Optional[str] = None


@dataclass
class FlowGraph:
    """Represents the complete flow graph"""

    processors: Dict[str, Processor] = field(default_factory=dict)
    connections: List[Connection] = field(default_factory=list)
    template_name: Optional[str] = None
    template_description: Optional[str] = None
    timestamp: Optional[str] = None

    def get_processor_by_id(self, processor_id: str) -> Optional[Processor]:
        """Get a processor by ID"""
        return self.processors.get(processor_id)

    def get_outgoing_connections(self, processor_id: str) -> List[Connection]:
        """Get all connections where this processor is the source"""
        return [conn for conn in self.connections if conn.source_id == processor_id]

    def get_incoming_connections(self, processor_id: str) -> List[Connection]:
        """Get all connections where this processor is the destination"""
        return [conn for conn in self.connections if conn.destination_id == processor_id]


class TemplateParser:
    """Parser for NiFi template XML files"""

    # Regex pattern for detecting EL expressions
    EL_PATTERN = re.compile(r"\$\{[^}]+\}")

    def __init__(self):
        self.flow_graph: Optional[FlowGraph] = None

    def parse_template(self, file_path: Path) -> FlowGraph:
        """
        Parse a NiFi template XML file and return a FlowGraph

        Args:
            file_path: Path to the template XML file

        Returns:
            FlowGraph object containing processors and connections

        Raises:
            FileNotFoundError: If template file doesn't exist
            etree.XMLSyntaxError: If XML is malformed
        """
        if not file_path.exists():
            raise FileNotFoundError(f"Template file not found: {file_path}")

        # Parse XML
        tree = etree.parse(str(file_path))
        root = tree.getroot()

        # Create flow graph
        flow_graph = FlowGraph()

        # Extract template metadata
        flow_graph.template_name = self._get_text(root, "name")
        flow_graph.template_description = self._get_text(root, "description")
        flow_graph.timestamp = self._get_text(root, "timestamp")

        # Get snippet element (contains processors and connections)
        snippet = root.find("snippet")
        if snippet is None:
            raise ValueError("Template missing <snippet> element")

        # Extract direct processors from snippet (if any)
        processors = snippet.findall("processors")
        for proc_elem in processors:
            processor = self.extract_processor_info(proc_elem)
            flow_graph.processors[processor.id] = processor

        # Extract direct connections from snippet (if any)
        connections = snippet.findall("connections")
        for conn_elem in connections:
            connection = self.extract_connection_info(conn_elem)
            flow_graph.connections.append(connection)

        # Recursively extract processors and connections from process groups
        for pg_elem in snippet.findall("processGroups"):
            self._parse_process_group_recursive(pg_elem, flow_graph)

        self.flow_graph = flow_graph
        return flow_graph

    def _parse_process_group_recursive(self, pg_elem: etree.Element, flow_graph: FlowGraph) -> None:
        """
        Recursively parse a process group and all nested groups

        Args:
            pg_elem: XML element representing a process group
            flow_graph: FlowGraph to add processors and connections to
        """
        # Process groups in templates have a <contents> element that contains
        # the actual processors, connections, and nested process groups
        contents = pg_elem.find("contents")
        if contents is None:
            # No contents, check for direct children (older template format)
            contents = pg_elem

        # Extract processors from this group
        for proc_elem in contents.findall("processors"):
            processor = self.extract_processor_info(proc_elem)
            flow_graph.processors[processor.id] = processor

        # Extract connections from this group
        for conn_elem in contents.findall("connections"):
            connection = self.extract_connection_info(conn_elem)
            flow_graph.connections.append(connection)

        # Recursively process nested groups
        for nested_pg in contents.findall("processGroups"):
            self._parse_process_group_recursive(nested_pg, flow_graph)

    def extract_processor_info(self, processor_elem: etree.Element) -> Processor:
        """
        Extract processor information from XML element

        Args:
            processor_elem: XML element representing a processor

        Returns:
            Processor object
        """
        # Extract basic info
        proc_id = self._get_text(processor_elem, "id", required=True)
        proc_name = self._get_text(processor_elem, "name", default="Unnamed")
        proc_type = self._get_text(processor_elem, "type", required=True)
        parent_group_id = self._get_text(processor_elem, "parentGroupId", default="")
        state = self._get_text(processor_elem, "state", default="STOPPED")

        # Extract position
        position = None
        pos_elem = processor_elem.find("position")
        if pos_elem is not None:
            x = self._get_text(pos_elem, "x")
            y = self._get_text(pos_elem, "y")
            if x and y:
                try:
                    position = (float(x), float(y))
                except ValueError:
                    pass

        # Extract config
        properties = {}
        relationships = {}
        scheduling_strategy = None
        scheduling_period = None
        comments = None

        config = processor_elem.find("config")
        if config is not None:
            # Extract properties
            props_elem = config.find("properties")
            if props_elem is not None:
                for entry in props_elem.findall("entry"):
                    key_elem = entry.find("key")
                    value_elem = entry.find("value")

                    if key_elem is not None:
                        key = key_elem.text or ""
                        value = value_elem.text if value_elem is not None else None
                        properties[key] = value

            # Extract scheduling info
            scheduling_strategy = self._get_text(config, "schedulingStrategy")
            scheduling_period = self._get_text(config, "schedulingPeriod")
            comments = self._get_text(config, "comments")

        # Extract relationships
        for rel_elem in processor_elem.findall("relationships"):
            rel_name = self._get_text(rel_elem, "name", default="")
            auto_terminate_text = self._get_text(rel_elem, "autoTerminate", default="false")
            auto_terminate = auto_terminate_text.lower() == "true"
            if rel_name:
                relationships[rel_name] = auto_terminate

        return Processor(
            id=proc_id,
            name=proc_name,
            type=proc_type,
            parent_group_id=parent_group_id,
            properties=properties,
            relationships=relationships,
            state=state,
            scheduling_strategy=scheduling_strategy,
            scheduling_period=scheduling_period,
            position=position,
            comments=comments,
        )

    def extract_connection_info(self, connection_elem: etree.Element) -> Connection:
        """
        Extract connection information from XML element

        Args:
            connection_elem: XML element representing a connection

        Returns:
            Connection object
        """
        conn_id = self._get_text(connection_elem, "id", required=True)
        conn_name = self._get_text(connection_elem, "name")
        parent_group_id = self._get_text(connection_elem, "parentGroupId")

        # Extract source
        source = connection_elem.find("source")
        if source is None:
            raise ValueError(f"Connection {conn_id} missing <source> element")

        source_id = self._get_text(source, "id", required=True)
        source_type = self._get_text(source, "type", default="PROCESSOR")

        # Extract destination
        destination = connection_elem.find("destination")
        if destination is None:
            raise ValueError(f"Connection {conn_id} missing <destination> element")

        dest_id = self._get_text(destination, "id", required=True)
        dest_type = self._get_text(destination, "type", default="PROCESSOR")

        # Extract selected relationships
        relationships = []
        for rel_elem in connection_elem.findall("selectedRelationships"):
            if rel_elem.text:
                relationships.append(rel_elem.text)

        return Connection(
            id=conn_id,
            source_id=source_id,
            source_type=source_type,
            destination_id=dest_id,
            destination_type=dest_type,
            relationships=relationships,
            name=conn_name,
            parent_group_id=parent_group_id,
        )

    def extract_el_expressions(
        self, flow_graph: Optional[FlowGraph] = None
    ) -> List[Tuple[str, str, str, str]]:
        """
        Extract all Expression Language expressions from a flow graph

        Args:
            flow_graph: FlowGraph to analyze (uses self.flow_graph if None)

        Returns:
            List of tuples: (processor_id, processor_name, property_name, expression)
        """
        if flow_graph is None:
            flow_graph = self.flow_graph

        if flow_graph is None:
            raise ValueError("No flow graph available. Call parse_template first.")

        expressions = []

        for processor in flow_graph.processors.values():
            for prop_name, prop_value in processor.properties.items():
                if prop_value and self.EL_PATTERN.search(prop_value):
                    expressions.append((processor.id, processor.name, prop_name, prop_value))

        return expressions

    def analyze_template(self, template_path: Path) -> Dict[str, Any]:
        """
        Analyze a template and return comprehensive statistics

        Args:
            template_path: Path to template file

        Returns:
            Dictionary with analysis results
        """
        flow_graph = self.parse_template(template_path)

        # Count processors by type
        processor_type_counts = {}
        for processor in flow_graph.processors.values():
            short_type = processor.get_short_type()
            processor_type_counts[short_type] = processor_type_counts.get(short_type, 0) + 1

        # Get unique processor types
        unique_types = sorted(set(p.get_short_type() for p in flow_graph.processors.values()))

        # Extract EL expressions
        el_expressions = self.extract_el_expressions(flow_graph)

        # Count relationships
        total_relationships = sum(len(p.relationships) for p in flow_graph.processors.values())
        auto_terminated = sum(
            1
            for p in flow_graph.processors.values()
            for auto_term in p.relationships.values()
            if auto_term
        )

        # Connection statistics
        connection_count = len(flow_graph.connections)

        # Extract unique EL functions used
        el_functions = set()
        for _, _, _, expr in el_expressions:
            # Find all function calls in format :functionName(
            func_matches = re.findall(r":(\w+)\(", expr)
            el_functions.update(func_matches)

        return {
            "template_name": flow_graph.template_name,
            "template_description": flow_graph.template_description,
            "timestamp": flow_graph.timestamp,
            "total_processors": len(flow_graph.processors),
            "processor_types": processor_type_counts,
            "unique_processor_types": unique_types,
            "total_connections": connection_count,
            "total_relationships": total_relationships,
            "auto_terminated_relationships": auto_terminated,
            "el_expressions": el_expressions,
            "el_expression_count": len(el_expressions),
            "unique_el_functions": sorted(el_functions),
            "flow_graph": flow_graph,
        }

    @staticmethod
    def _get_text(
        element: etree.Element, tag: str, default: Optional[str] = None, required: bool = False
    ) -> Optional[str]:
        """
        Safely extract text content from an XML element

        Args:
            element: Parent element
            tag: Tag name to find
            default: Default value if not found
            required: Raise ValueError if not found and required=True

        Returns:
            Text content or default value
        """
        child = element.find(tag)
        if child is not None:
            return child.text

        if required:
            raise ValueError(f"Required element <{tag}> not found")

        return default


def parse_template(file_path: Path) -> FlowGraph:
    """
    Convenience function to parse a template file

    Args:
        file_path: Path to template XML file

    Returns:
        FlowGraph object
    """
    parser = TemplateParser()
    return parser.parse_template(file_path)


def analyze_template(template_path: Path) -> Dict[str, Any]:
    """
    Convenience function to analyze a template file

    Args:
        template_path: Path to template XML file

    Returns:
        Dictionary with analysis results
    """
    parser = TemplateParser()
    return parser.analyze_template(template_path)
