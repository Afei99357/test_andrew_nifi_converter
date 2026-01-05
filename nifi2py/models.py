"""
Core data models for nifi2py - NiFi to Python converter.

This module provides Pydantic models representing NiFi concepts including
FlowFiles, Processors, Connections, Process Groups, and validation results.
"""

from __future__ import annotations

import hashlib
import uuid as uuid_module
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field, field_validator, computed_field


__all__ = [
    "FlowFile",
    "Processor",
    "Connection",
    "ProcessGroup",
    "FlowGraph",
    "ProvenanceEvent",
    "ConversionResult",
    "ValidationResult",
    "Position",
    "Relationship",
]


class Position(BaseModel):
    """Represents the x,y coordinates of a component on the NiFi canvas."""

    x: float = Field(..., description="X coordinate on canvas")
    y: float = Field(..., description="Y coordinate on canvas")

    def __repr__(self) -> str:
        return f"Position(x={self.x}, y={self.y})"


class Relationship(BaseModel):
    """Represents a processor relationship/output."""

    name: str = Field(..., description="Relationship name (e.g., 'success', 'failure')")
    auto_terminate: bool = Field(
        default=False, description="Whether this relationship auto-terminates"
    )

    def __repr__(self) -> str:
        return f"Relationship(name='{self.name}', auto_terminate={self.auto_terminate})"


class FlowFile(BaseModel):
    """
    Represents a NiFi FlowFile - the fundamental unit of data flow.

    A FlowFile consists of:
    - Content: The actual data (bytes)
    - Attributes: Metadata key-value pairs
    - UUID: Unique identifier

    Example:
        >>> ff = FlowFile(
        ...     content=b"Hello World",
        ...     attributes={"filename": "test.txt", "mime.type": "text/plain"}
        ... )
        >>> ff.uuid  # Auto-generated
        >>> ff.size
        11
    """

    content: bytes = Field(default=b"", description="The actual data content")
    attributes: Dict[str, str] = Field(
        default_factory=dict, description="Metadata key-value pairs"
    )
    uuid: str = Field(
        default_factory=lambda: str(uuid_module.uuid4()),
        description="Unique FlowFile identifier",
    )

    class Config:
        arbitrary_types_allowed = True

    @computed_field
    @property
    def size(self) -> int:
        """Return the size of the content in bytes."""
        return len(self.content)

    @computed_field
    @property
    def content_hash(self) -> str:
        """Return SHA-256 hash of the content."""
        return hashlib.sha256(self.content).hexdigest()

    def clone(
        self,
        content: Optional[bytes] = None,
        attributes: Optional[Dict[str, str]] = None,
    ) -> FlowFile:
        """
        Create a clone of this FlowFile with optional overrides.

        Args:
            content: New content (if None, uses current content)
            attributes: New attributes (if None, copies current attributes)

        Returns:
            New FlowFile instance with new UUID
        """
        return FlowFile(
            content=content if content is not None else self.content,
            attributes=attributes if attributes is not None else self.attributes.copy(),
        )

    def update_attributes(self, **kwargs: str) -> FlowFile:
        """
        Update attributes in-place and return self for chaining.

        Args:
            **kwargs: Attribute key-value pairs to add/update

        Returns:
            Self for method chaining

        Example:
            >>> ff.update_attributes(filename="new.txt", processed="true")
        """
        self.attributes.update(kwargs)
        return self

    def get_attribute(self, key: str, default: str = "") -> str:
        """
        Get an attribute value with optional default.

        Args:
            key: Attribute key
            default: Default value if key not found

        Returns:
            Attribute value or default
        """
        return self.attributes.get(key, default)

    def __repr__(self) -> str:
        return (
            f"FlowFile(uuid='{self.uuid[:8]}...', size={self.size}, "
            f"attributes={len(self.attributes)} keys)"
        )


class Processor(BaseModel):
    """
    Represents a NiFi Processor configuration.

    Processors are the workhorses of NiFi - they ingest, transform, route,
    and export data. Each processor has properties (configuration) and
    relationships (outputs).

    Example:
        >>> proc = Processor(
        ...     id="update-1",
        ...     name="Add Timestamp",
        ...     type="org.apache.nifi.processors.attributes.UpdateAttribute",
        ...     properties={"timestamp": "${now():format('yyyy-MM-dd')}"},
        ...     relationships=[
        ...         Relationship(name="success", auto_terminate=False)
        ...     ]
        ... )
    """

    id: str = Field(..., description="Unique processor identifier")
    name: str = Field(..., description="Human-readable processor name")
    type: str = Field(
        ...,
        description="Fully qualified class name (e.g., 'org.apache.nifi.processors.standard.UpdateAttribute')",
    )
    properties: Dict[str, Optional[str]] = Field(
        default_factory=dict, description="Processor configuration properties"
    )
    relationships: List[Relationship] = Field(
        default_factory=list, description="Output relationships"
    )
    state: str = Field(default="STOPPED", description="Processor state (RUNNING, STOPPED, etc.)")
    position: Optional[Position] = Field(
        default=None, description="Position on canvas"
    )
    parent_group_id: Optional[str] = Field(
        default=None, description="Parent process group ID"
    )
    scheduling_period: str = Field(
        default="0 sec", description="Scheduling period (e.g., '30 sec', '5 min')"
    )
    scheduling_strategy: str = Field(
        default="TIMER_DRIVEN", description="Scheduling strategy (TIMER_DRIVEN, CRON_DRIVEN, EVENT_DRIVEN)"
    )
    concurrent_tasks: int = Field(
        default=1, description="Number of concurrent tasks"
    )
    comments: str = Field(default="", description="Processor comments/notes")

    @field_validator("state")
    @classmethod
    def validate_state(cls, v: str) -> str:
        """Validate processor state is valid."""
        valid_states = {"RUNNING", "STOPPED", "DISABLED", "INVALID"}
        if v.upper() not in valid_states:
            raise ValueError(f"State must be one of {valid_states}")
        return v.upper()

    @computed_field
    @property
    def processor_simple_type(self) -> str:
        """Extract simple processor type name from fully qualified class name."""
        return self.type.split(".")[-1] if "." in self.type else self.type

    def get_property(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get a processor property value.

        Args:
            key: Property key
            default: Default value if not found

        Returns:
            Property value or default
        """
        return self.properties.get(key, default)

    def get_relationship_names(self) -> Set[str]:
        """Get set of all relationship names."""
        return {rel.name for rel in self.relationships}

    def __repr__(self) -> str:
        return (
            f"Processor(id='{self.id}', name='{self.name}', "
            f"type='{self.processor_simple_type}', state='{self.state}')"
        )


class Connection(BaseModel):
    """
    Represents a connection between NiFi components.

    Connections link processors together, defining how FlowFiles flow
    through the system. Each connection specifies which relationships
    from the source processor route to the destination.

    Example:
        >>> conn = Connection(
        ...     id="conn-1",
        ...     source_id="proc-1",
        ...     destination_id="proc-2",
        ...     selected_relationships=["success"]
        ... )
    """

    id: str = Field(..., description="Unique connection identifier")
    source_id: str = Field(..., description="Source processor/port ID")
    destination_id: str = Field(..., description="Destination processor/port ID")
    selected_relationships: List[str] = Field(
        default_factory=list,
        description="Relationships from source that use this connection",
    )
    name: Optional[str] = Field(default=None, description="Optional connection name")
    parent_group_id: Optional[str] = Field(
        default=None, description="Parent process group ID"
    )
    back_pressure_data_size_threshold: str = Field(
        default="1 GB", description="Back pressure data size threshold"
    )
    back_pressure_object_threshold: int = Field(
        default=10000, description="Back pressure object count threshold"
    )
    flow_file_expiration: str = Field(
        default="0 sec", description="FlowFile expiration timeout"
    )

    def __repr__(self) -> str:
        relationships = ", ".join(self.selected_relationships)
        return (
            f"Connection(id='{self.id}', source='{self.source_id}' -> "
            f"destination='{self.destination_id}', relationships=[{relationships}])"
        )


class ProcessGroup(BaseModel):
    """
    Represents a NiFi Process Group - a hierarchical container for flow components.

    Process groups organize flows into logical units and can be nested.
    They contain processors, connections, and other process groups.

    Example:
        >>> group = ProcessGroup(
        ...     id="group-1",
        ...     name="Data Ingestion",
        ...     processors=[proc1, proc2],
        ...     connections=[conn1],
        ...     process_groups=[]
        ... )
    """

    id: str = Field(..., description="Unique process group identifier")
    name: str = Field(..., description="Process group name")
    processors: List[Processor] = Field(
        default_factory=list, description="Processors in this group"
    )
    connections: List[Connection] = Field(
        default_factory=list, description="Connections in this group"
    )
    process_groups: List[ProcessGroup] = Field(
        default_factory=list, description="Nested process groups"
    )
    parent_group_id: Optional[str] = Field(
        default=None, description="Parent process group ID (None for root)"
    )
    position: Optional[Position] = Field(
        default=None, description="Position on canvas"
    )
    comments: str = Field(default="", description="Process group comments")

    @computed_field
    @property
    def total_processor_count(self) -> int:
        """Recursively count all processors in this group and subgroups."""
        count = len(self.processors)
        for subgroup in self.process_groups:
            count += subgroup.total_processor_count
        return count

    @computed_field
    @property
    def total_connection_count(self) -> int:
        """Recursively count all connections in this group and subgroups."""
        count = len(self.connections)
        for subgroup in self.process_groups:
            count += subgroup.total_connection_count
        return count

    def get_processor_by_id(self, processor_id: str) -> Optional[Processor]:
        """
        Find a processor by ID in this group or any subgroup.

        Args:
            processor_id: Processor ID to find

        Returns:
            Processor if found, None otherwise
        """
        # Check this group
        for proc in self.processors:
            if proc.id == processor_id:
                return proc

        # Recursively check subgroups
        for subgroup in self.process_groups:
            result = subgroup.get_processor_by_id(processor_id)
            if result:
                return result

        return None

    def get_connection_by_id(self, connection_id: str) -> Optional[Connection]:
        """
        Find a connection by ID in this group or any subgroup.

        Args:
            connection_id: Connection ID to find

        Returns:
            Connection if found, None otherwise
        """
        # Check this group
        for conn in self.connections:
            if conn.id == connection_id:
                return conn

        # Recursively check subgroups
        for subgroup in self.process_groups:
            result = subgroup.get_connection_by_id(connection_id)
            if result:
                return result

        return None

    def get_all_processors(self) -> List[Processor]:
        """Get all processors in this group and all subgroups."""
        all_processors = list(self.processors)
        for subgroup in self.process_groups:
            all_processors.extend(subgroup.get_all_processors())
        return all_processors

    def get_all_connections(self) -> List[Connection]:
        """Get all connections in this group and all subgroups."""
        all_connections = list(self.connections)
        for subgroup in self.process_groups:
            all_connections.extend(subgroup.get_all_connections())
        return all_connections

    def __repr__(self) -> str:
        return (
            f"ProcessGroup(id='{self.id}', name='{self.name}', "
            f"processors={len(self.processors)}, connections={len(self.connections)}, "
            f"subgroups={len(self.process_groups)})"
        )


class FlowGraph(BaseModel):
    """
    Represents the complete NiFi flow graph.

    This is the top-level container for a NiFi flow, containing the root
    process group and providing helper methods for navigation and analysis.

    Example:
        >>> graph = FlowGraph(root_group=root_process_group)
        >>> proc = graph.get_processor_by_id("proc-1")
        >>> graph.total_processors
        42
    """

    root_group: ProcessGroup = Field(..., description="Root process group")
    name: Optional[str] = Field(default=None, description="Flow name")
    description: Optional[str] = Field(default=None, description="Flow description")
    timestamp: Optional[datetime] = Field(
        default=None, description="Flow creation/export timestamp"
    )

    @computed_field
    @property
    def total_processors(self) -> int:
        """Total number of processors in the entire flow."""
        return self.root_group.total_processor_count

    @computed_field
    @property
    def total_connections(self) -> int:
        """Total number of connections in the entire flow."""
        return self.root_group.total_connection_count

    def get_processor_by_id(self, processor_id: str) -> Optional[Processor]:
        """
        Find a processor by ID anywhere in the flow.

        Args:
            processor_id: Processor ID to find

        Returns:
            Processor if found, None otherwise
        """
        return self.root_group.get_processor_by_id(processor_id)

    def get_all_processors(self) -> List[Processor]:
        """Get all processors in the flow."""
        return self.root_group.get_all_processors()

    def get_all_connections(self) -> List[Connection]:
        """Get all connections in the flow."""
        return self.root_group.get_all_connections()

    def get_processor_types(self) -> Dict[str, int]:
        """
        Get count of each processor type in the flow.

        Returns:
            Dictionary mapping processor types to counts

        Example:
            >>> graph.get_processor_types()
            {'UpdateAttribute': 15, 'LogMessage': 8, ...}
        """
        type_counts: Dict[str, int] = {}
        for proc in self.get_all_processors():
            simple_type = proc.processor_simple_type
            type_counts[simple_type] = type_counts.get(simple_type, 0) + 1
        return type_counts

    def get_connection_graph(self) -> Dict[str, List[str]]:
        """
        Get adjacency list representation of the flow graph.

        Returns:
            Dictionary mapping processor IDs to lists of connected processor IDs

        Example:
            >>> graph.get_connection_graph()
            {'proc-1': ['proc-2', 'proc-3'], 'proc-2': ['proc-4'], ...}
        """
        adjacency: Dict[str, List[str]] = {}

        for conn in self.get_all_connections():
            if conn.source_id not in adjacency:
                adjacency[conn.source_id] = []
            adjacency[conn.source_id].append(conn.destination_id)

        return adjacency

    def get_source_processors(self) -> List[Processor]:
        """
        Get processors that are flow sources (no incoming connections).

        Returns:
            List of source processors
        """
        all_processors = self.get_all_processors()
        all_connections = self.get_all_connections()

        # Get all destination IDs
        destination_ids = {conn.destination_id for conn in all_connections}

        # Processors that are never destinations are sources
        return [proc for proc in all_processors if proc.id not in destination_ids]

    def get_sink_processors(self) -> List[Processor]:
        """
        Get processors that are flow sinks (no outgoing connections or all auto-terminate).

        Returns:
            List of sink processors
        """
        all_processors = self.get_all_processors()
        all_connections = self.get_all_connections()

        # Get all source IDs that have non-auto-terminating connections
        source_ids = {conn.source_id for conn in all_connections}

        # Processors that are never sources or have all auto-terminating relationships
        sinks = []
        for proc in all_processors:
            if proc.id not in source_ids:
                # No outgoing connections at all
                sinks.append(proc)
            else:
                # Check if all relationships are auto-terminate
                if all(rel.auto_terminate for rel in proc.relationships):
                    sinks.append(proc)

        return sinks

    def __repr__(self) -> str:
        return (
            f"FlowGraph(name='{self.name}', processors={self.total_processors}, "
            f"connections={self.total_connections})"
        )


class ProvenanceEvent(BaseModel):
    """
    Represents a NiFi Provenance Event - execution history record.

    Provenance events track the lifecycle of FlowFiles through the system,
    recording what happened, when, and with what data. These are crucial
    for validation and debugging.

    Event Types:
    - CREATE: FlowFile was created
    - RECEIVE: FlowFile was received from external source
    - SEND: FlowFile was sent to external destination
    - FETCH: FlowFile content was fetched
    - DROP: FlowFile was removed from flow
    - ROUTE: FlowFile was routed to relationship
    - FORK: FlowFile was cloned
    - JOIN: FlowFiles were merged
    - CLONE: FlowFile was cloned
    - CONTENT_MODIFIED: FlowFile content was modified
    - ATTRIBUTES_MODIFIED: FlowFile attributes were modified

    Example:
        >>> event = ProvenanceEvent(
        ...     event_id=12345,
        ...     event_type="CONTENT_MODIFIED",
        ...     processor_id="proc-1",
        ...     flowfile_uuid="abc-123",
        ...     timestamp=datetime.now(),
        ...     attributes={"filename": "test.txt"}
        ... )
    """

    event_id: int = Field(..., description="Unique event identifier")
    event_type: str = Field(..., description="Event type (CREATE, SEND, RECEIVE, etc.)")
    processor_id: str = Field(..., description="ID of processor that generated event")
    processor_name: Optional[str] = Field(
        default=None, description="Name of processor that generated event"
    )
    processor_type: Optional[str] = Field(
        default=None, description="Type of processor that generated event"
    )
    flowfile_uuid: str = Field(..., description="FlowFile UUID")
    timestamp: datetime = Field(..., description="Event timestamp")
    event_duration_millis: int = Field(
        default=0, description="Event duration in milliseconds"
    )
    input_content_claim: Optional[str] = Field(
        default=None, description="Input content claim identifier"
    )
    output_content_claim: Optional[str] = Field(
        default=None, description="Output content claim identifier"
    )
    attributes: Dict[str, str] = Field(
        default_factory=dict, description="FlowFile attributes at time of event"
    )
    previous_attributes: Dict[str, str] = Field(
        default_factory=dict, description="Previous FlowFile attributes (for ATTRIBUTES_MODIFIED)"
    )
    updated_attributes: Dict[str, str] = Field(
        default_factory=dict, description="Updated attributes (for ATTRIBUTES_MODIFIED)"
    )
    parent_uuids: List[str] = Field(
        default_factory=list, description="Parent FlowFile UUIDs (for FORK, JOIN)"
    )
    child_uuids: List[str] = Field(
        default_factory=list, description="Child FlowFile UUIDs (for FORK, CLONE)"
    )
    relationship: Optional[str] = Field(
        default=None, description="Relationship name (for ROUTE)"
    )
    details: Optional[str] = Field(default=None, description="Additional event details")

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        """Validate event type is valid."""
        valid_types = {
            "CREATE",
            "RECEIVE",
            "SEND",
            "FETCH",
            "DROP",
            "ROUTE",
            "FORK",
            "JOIN",
            "CLONE",
            "CONTENT_MODIFIED",
            "ATTRIBUTES_MODIFIED",
            "EXPIRE",
            "DOWNLOAD",
            "REPLAY",
        }
        if v.upper() not in valid_types:
            raise ValueError(f"Event type must be one of {valid_types}")
        return v.upper()

    @computed_field
    @property
    def content_modified(self) -> bool:
        """Check if this event modified content."""
        return (
            self.input_content_claim != self.output_content_claim
            and self.output_content_claim is not None
        )

    @computed_field
    @property
    def attributes_modified(self) -> bool:
        """Check if this event modified attributes."""
        return bool(self.updated_attributes) or self.event_type == "ATTRIBUTES_MODIFIED"

    def __repr__(self) -> str:
        return (
            f"ProvenanceEvent(id={self.event_id}, type='{self.event_type}', "
            f"processor='{self.processor_id}', flowfile='{self.flowfile_uuid[:8]}...')"
        )


class ConversionResult(BaseModel):
    """
    Represents the result of converting a NiFi processor to Python code.

    When converting a processor, we generate Python code that replicates
    its behavior. This model captures the generated code along with metadata
    about the conversion.

    Example:
        >>> result = ConversionResult(
        ...     processor_id="proc-1",
        ...     processor_name="Add Timestamp",
        ...     function_name="process_add_timestamp",
        ...     function_code="def process_add_timestamp(flowfile):\\n    ...",
        ...     is_stub=False,
        ...     dependencies=["datetime", "typing"],
        ...     notes="Successfully converted UpdateAttribute processor"
        ... )
    """

    processor_id: str = Field(..., description="Original processor ID")
    processor_name: str = Field(..., description="Original processor name")
    processor_type: str = Field(..., description="Original processor type")
    function_name: str = Field(..., description="Generated Python function name")
    function_code: str = Field(..., description="Generated Python function code")
    is_stub: bool = Field(
        default=False,
        description="True if this is a stub requiring manual implementation",
    )
    dependencies: List[str] = Field(
        default_factory=list, description="Required Python imports/modules"
    )
    notes: Optional[str] = Field(
        default=None, description="Migration hints or implementation notes"
    )
    warnings: List[str] = Field(
        default_factory=list, description="Conversion warnings or caveats"
    )
    coverage_percentage: int = Field(
        default=100,
        ge=0,
        le=100,
        description="Estimated conversion coverage (0-100%)",
    )

    @computed_field
    @property
    def is_complete(self) -> bool:
        """Check if conversion is complete (not a stub and 100% coverage)."""
        return not self.is_stub and self.coverage_percentage == 100

    def add_warning(self, warning: str) -> ConversionResult:
        """
        Add a warning to this conversion result.

        Args:
            warning: Warning message

        Returns:
            Self for method chaining
        """
        self.warnings.append(warning)
        return self

    def add_dependency(self, *deps: str) -> ConversionResult:
        """
        Add dependencies to this conversion result.

        Args:
            *deps: Dependency names

        Returns:
            Self for method chaining
        """
        for dep in deps:
            if dep not in self.dependencies:
                self.dependencies.append(dep)
        return self

    def __repr__(self) -> str:
        status = "stub" if self.is_stub else f"{self.coverage_percentage}% complete"
        return (
            f"ConversionResult(processor='{self.processor_name}', "
            f"function='{self.function_name}', status={status})"
        )


class ValidationResult(BaseModel):
    """
    Represents the result of validating Python code against NiFi provenance.

    Validation compares the output of generated Python code against actual
    NiFi provenance data to ensure parity. This model captures the comparison
    results for a single FlowFile/event.

    Example:
        >>> result = ValidationResult(
        ...     processor_id="proc-1",
        ...     event_id=12345,
        ...     content_match=True,
        ...     attributes_match=True,
        ...     expected_content_hash="abc123...",
        ...     actual_content_hash="abc123...",
        ...     expected_attributes={"filename": "test.txt"},
        ...     actual_attributes={"filename": "test.txt"}
        ... )
        >>> result.passed
        True
    """

    processor_id: str = Field(..., description="Processor ID being validated")
    processor_name: Optional[str] = Field(
        default=None, description="Processor name being validated"
    )
    event_id: int = Field(..., description="Provenance event ID")
    flowfile_uuid: Optional[str] = Field(
        default=None, description="FlowFile UUID"
    )
    content_match: bool = Field(
        ..., description="True if content matches NiFi output"
    )
    attributes_match: bool = Field(
        ..., description="True if attributes match NiFi output"
    )
    expected_content_hash: str = Field(
        ..., description="SHA-256 hash of NiFi output content"
    )
    actual_content_hash: str = Field(
        ..., description="SHA-256 hash of Python output content"
    )
    expected_attributes: Dict[str, str] = Field(
        default_factory=dict, description="Expected attributes from NiFi"
    )
    actual_attributes: Dict[str, str] = Field(
        default_factory=dict, description="Actual attributes from Python"
    )
    attribute_diffs: Dict[str, Tuple[str, str]] = Field(
        default_factory=dict,
        description="Attribute differences: key -> (expected, actual)",
    )
    error: Optional[str] = Field(
        default=None, description="Error message if validation failed"
    )
    timestamp: datetime = Field(
        default_factory=datetime.now, description="Validation timestamp"
    )

    @computed_field
    @property
    def passed(self) -> bool:
        """Check if validation passed (content and attributes match)."""
        return self.content_match and self.attributes_match and self.error is None

    @computed_field
    @property
    def attribute_match_percentage(self) -> float:
        """Calculate percentage of attributes that match."""
        if not self.expected_attributes:
            return 100.0

        total = len(self.expected_attributes)
        matches = total - len(self.attribute_diffs)
        return (matches / total) * 100.0

    def get_content_diff_summary(self) -> str:
        """Get summary of content differences."""
        if self.content_match:
            return "Content matches"
        return f"Content mismatch: expected {self.expected_content_hash}, got {self.actual_content_hash}"

    def get_attribute_diff_summary(self) -> str:
        """Get summary of attribute differences."""
        if self.attributes_match:
            return "All attributes match"

        diff_lines = []
        for key, (expected, actual) in self.attribute_diffs.items():
            diff_lines.append(f"  {key}: expected='{expected}', actual='{actual}'")

        missing_in_actual = set(self.expected_attributes.keys()) - set(
            self.actual_attributes.keys()
        )
        for key in missing_in_actual:
            diff_lines.append(f"  {key}: MISSING in actual output")

        extra_in_actual = set(self.actual_attributes.keys()) - set(
            self.expected_attributes.keys()
        )
        for key in extra_in_actual:
            diff_lines.append(
                f"  {key}: EXTRA in actual output (value='{self.actual_attributes[key]}')"
            )

        return "Attribute differences:\n" + "\n".join(diff_lines)

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        return (
            f"ValidationResult(processor='{self.processor_id}', event={self.event_id}, "
            f"status={status}, content_match={self.content_match}, "
            f"attributes_match={self.attributes_match})"
        )
