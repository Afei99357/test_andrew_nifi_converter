"""
Code Generator for NiFi to Python Conversion

Generates executable Python modules from NiFi flow graphs using Jinja2 templates.
Supports module, notebook, and package output formats.

Author: nifi2py
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from jinja2 import Environment, FileSystemLoader, Template

# Import models - use unified models from models.py
try:
    from .models import FlowGraph, Processor, Connection
    from .template_parser import TemplateParser
except ImportError:
    # Fallback for direct execution
    from models import FlowGraph, Processor, Connection
    from template_parser import TemplateParser


__all__ = ["GenerationResult", "CodeGenerator"]


@dataclass
class GenerationResult:
    """
    Result of code generation from a NiFi flow.

    Contains the generated Python code along with metadata about the conversion,
    including coverage statistics and warnings.

    Attributes:
        code: Generated Python code
        file_name: Suggested output filename
        dependencies: Python imports needed
        processor_count: Total processors in flow
        connection_count: Total connections in flow
        stub_count: Number of stub processors requiring implementation
        coverage_percentage: Percentage of processors fully converted
        warnings: Conversion warnings
        metadata: Additional metadata
    """

    code: str
    file_name: str
    dependencies: List[str] = field(default_factory=list)
    processor_count: int = 0
    connection_count: int = 0
    stub_count: int = 0
    coverage_percentage: float = 0.0
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, str] = field(default_factory=dict)

    def save(self, output_path: Path) -> None:
        """
        Save generated code to file.

        Args:
            output_path: Path where code should be saved
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(self.code, encoding="utf-8")

    def add_warning(self, warning: str) -> GenerationResult:
        """
        Add a warning to the result.

        Args:
            warning: Warning message

        Returns:
            Self for method chaining
        """
        if warning not in self.warnings:
            self.warnings.append(warning)
        return self

    def add_dependency(self, *deps: str) -> GenerationResult:
        """
        Add dependencies to the result.

        Args:
            *deps: Dependency names

        Returns:
            Self for method chaining
        """
        for dep in deps:
            if dep not in self.dependencies:
                self.dependencies.append(dep)
        return self

    @property
    def is_complete(self) -> bool:
        """Check if conversion is complete (no stubs)."""
        return self.stub_count == 0

    def __repr__(self) -> str:
        return (
            f"GenerationResult(file='{self.file_name}', "
            f"processors={self.processor_count}, coverage={self.coverage_percentage:.1f}%)"
        )


class CodeGenerator:
    """
    Generates executable Python code from NiFi flow graphs.

    Supports multiple output formats:
    - module: Single Python module (.py)
    - notebook: Jupyter notebook (.ipynb)
    - package: Python package structure

    Example:
        >>> generator = CodeGenerator(output_format="module")
        >>> result = generator.generate_from_template(Path("flow.xml"))
        >>> result.save(Path("output.py"))
    """

    SUPPORTED_FORMATS = {"module", "notebook", "package"}

    def __init__(
        self, output_format: str = "module", template_dir: Optional[Path] = None
    ):
        """
        Initialize code generator.

        Args:
            output_format: Output format ("module", "notebook", or "package")
            template_dir: Custom template directory (default: built-in templates)

        Raises:
            ValueError: If output_format is not supported
        """
        if output_format not in self.SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported output format: {output_format}. "
                f"Must be one of {self.SUPPORTED_FORMATS}"
            )

        self.output_format = output_format

        # Setup Jinja2 environment
        if template_dir is None:
            # Use built-in templates
            template_dir = Path(__file__).parent / "templates"

        if not template_dir.exists():
            raise ValueError(f"Template directory not found: {template_dir}")

        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        # Load the main template
        self.template = self.env.get_template("module.py.j2")

        # Registry for processor converters (will be populated by converter plugins)
        self.converter_registry: Dict[str, callable] = {}

        # Import built-in converters if available
        self._load_builtin_converters()

    def _load_builtin_converters(self) -> None:
        """Load built-in processor converters if available."""
        try:
            # Try to import converters module
            from . import converters

            # Auto-register converters from the converters module
            # This will be implemented when converters are added
            pass
        except ImportError:
            # No converters module yet - that's OK
            pass

    def generate(self, flow_graph: FlowGraph) -> GenerationResult:
        """
        Generate Python code from a FlowGraph.

        Args:
            flow_graph: Parsed NiFi flow graph

        Returns:
            GenerationResult with generated code and metadata
        """
        # Extract flow metadata
        flow_name = flow_graph.name or "Unnamed Flow"
        template_name = getattr(flow_graph, "template_name", flow_name)

        # Get all processors and connections
        all_processors = flow_graph.get_all_processors()
        all_connections = flow_graph.get_all_connections()

        processor_count = len(all_processors)
        connection_count = len(all_connections)

        # Convert processors to functions
        processor_functions = []
        processor_map = {}
        processor_metadata = {}
        stub_count = 0
        warnings = []
        dependencies = set()

        for processor in all_processors:
            func_code, is_stub, proc_deps, proc_warnings = self._convert_processor(
                processor
            )
            processor_functions.append(func_code)

            # Generate safe function name from processor ID
            func_name = self._make_function_name(processor.id, processor.name)
            processor_map[processor.id] = func_name

            processor_metadata[processor.id] = {
                "name": processor.name,
                "type": processor.processor_simple_type,
                "is_stub": is_stub,
            }

            if is_stub:
                stub_count += 1

            dependencies.update(proc_deps)
            warnings.extend(proc_warnings)

        # Build connection graph
        connection_graph = self._build_connection_graph(all_connections)

        # Calculate coverage
        coverage_percentage = (
            ((processor_count - stub_count) / processor_count * 100.0)
            if processor_count > 0
            else 0.0
        )

        # Collect helper functions (EL support functions)
        helper_functions = self._get_helper_functions()

        # Prepare additional imports
        additional_imports = [f"import {dep}" for dep in sorted(dependencies)]

        # Render template
        code = self.template.render(
            flow_name=flow_name,
            template_name=template_name,
            timestamp=datetime.now().isoformat(),
            processor_count=processor_count,
            connection_count=connection_count,
            stub_count=stub_count,
            coverage_percentage=f"{coverage_percentage:.1f}",
            processor_functions=processor_functions,
            connection_graph=connection_graph,
            processor_map=processor_map,
            processor_metadata=processor_metadata,
            helper_functions=helper_functions,
            additional_imports=additional_imports,
        )

        # Generate filename
        safe_name = re.sub(r"[^a-z0-9_]+", "_", flow_name.lower())
        file_name = f"{safe_name}.py"

        return GenerationResult(
            code=code,
            file_name=file_name,
            dependencies=list(dependencies),
            processor_count=processor_count,
            connection_count=connection_count,
            stub_count=stub_count,
            coverage_percentage=coverage_percentage,
            warnings=warnings,
            metadata={
                "flow_name": flow_name,
                "template_name": template_name,
                "timestamp": datetime.now().isoformat(),
            },
        )

    def generate_from_template(self, template_path: Path) -> GenerationResult:
        """
        Parse a NiFi template and generate Python code.

        Args:
            template_path: Path to NiFi template XML file

        Returns:
            GenerationResult with generated code and metadata

        Raises:
            FileNotFoundError: If template file doesn't exist
        """
        if not template_path.exists():
            raise FileNotFoundError(f"Template not found: {template_path}")

        # Parse template using TemplateParser
        parser = TemplateParser()
        old_flow_graph = parser.parse_template(template_path)

        # Convert old FlowGraph to new FlowGraph model
        flow_graph = self._convert_flow_graph(old_flow_graph)

        # Store template metadata
        flow_graph.name = old_flow_graph.template_name
        flow_graph.description = old_flow_graph.template_description

        return self.generate(flow_graph)

    def _convert_flow_graph(self, old_flow_graph) -> FlowGraph:
        """
        Convert old template_parser FlowGraph to new models.FlowGraph.

        Args:
            old_flow_graph: FlowGraph from template_parser

        Returns:
            FlowGraph from models
        """
        from .models import (
            ProcessGroup,
            Processor as NewProcessor,
            Connection as NewConnection,
            Relationship,
            Position,
        )

        # Convert processors
        new_processors = []
        for old_proc in old_flow_graph.processors.values():
            # Convert relationships
            relationships = [
                Relationship(name=rel_name, auto_terminate=auto_term)
                for rel_name, auto_term in old_proc.relationships.items()
            ]

            # Convert position
            position = None
            if old_proc.position:
                position = Position(x=old_proc.position[0], y=old_proc.position[1])

            new_proc = NewProcessor(
                id=old_proc.id,
                name=old_proc.name,
                type=old_proc.type,
                properties=old_proc.properties,
                relationships=relationships,
                state=old_proc.state,
                position=position,
                parent_group_id=old_proc.parent_group_id,
                scheduling_period=old_proc.scheduling_period or "0 sec",
                scheduling_strategy=old_proc.scheduling_strategy or "TIMER_DRIVEN",
                comments=old_proc.comments or "",
            )
            new_processors.append(new_proc)

        # Convert connections
        new_connections = []
        for old_conn in old_flow_graph.connections:
            new_conn = NewConnection(
                id=old_conn.id,
                source_id=old_conn.source_id,
                destination_id=old_conn.destination_id,
                selected_relationships=old_conn.relationships,
                name=old_conn.name,
                parent_group_id=old_conn.parent_group_id,
            )
            new_connections.append(new_conn)

        # Create root process group
        root_group = ProcessGroup(
            id="root",
            name=old_flow_graph.template_name or "Root",
            processors=new_processors,
            connections=new_connections,
            process_groups=[],
        )

        return FlowGraph(
            root_group=root_group,
            name=old_flow_graph.template_name,
            description=old_flow_graph.template_description,
        )

    def _convert_processor(
        self, processor: Processor
    ) -> Tuple[str, bool, Set[str], List[str]]:
        """
        Convert a processor to Python function code.

        Args:
            processor: Processor to convert

        Returns:
            Tuple of (function_code, is_stub, dependencies, warnings)
        """
        processor_type = processor.processor_simple_type
        func_name = self._make_function_name(processor.id, processor.name)

        # Check if we have a converter for this processor type
        if processor_type in self.converter_registry:
            converter = self.converter_registry[processor_type]
            return converter(processor, func_name)

        # No converter available - generate stub
        return self._generate_stub(processor, func_name)

    def _generate_stub(
        self, processor: Processor, func_name: str
    ) -> Tuple[str, bool, Set[str], List[str]]:
        """
        Generate a stub function for an unsupported processor.

        Args:
            processor: Processor to stub
            func_name: Function name to generate

        Returns:
            Tuple of (function_code, is_stub=True, dependencies, warnings)
        """
        warnings = [
            f"Processor '{processor.name}' ({processor.processor_simple_type}) "
            f"has no converter - generated stub"
        ]

        # Get relationship names
        rel_names = [rel.name for rel in processor.relationships]

        # Generate stub code
        stub_code = f'''def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    """
    STUB: {processor.name} ({processor.processor_simple_type})

    Original processor ID: {processor.id}
    Type: {processor.type}

    Properties:
{self._format_properties(processor.properties)}

    Relationships: {', '.join(rel_names) if rel_names else 'None'}

    TODO: Implement this processor's logic
    """
    logger.warning("STUB: {func_name} not implemented - passing through unchanged")

    # Default: route to first relationship or 'success'
    default_rel = "{rel_names[0] if rel_names else 'success'}"
    return {{default_rel: [flowfile]}}'''

        return stub_code, True, set(), warnings

    def _format_properties(self, properties: Dict[str, Optional[str]]) -> str:
        """Format processor properties for docstring."""
        if not properties:
            return "    (none)"

        lines = []
        for key, value in properties.items():
            value_str = str(value) if value is not None else "(null)"
            # Truncate long values
            if len(value_str) > 60:
                value_str = value_str[:57] + "..."
            lines.append(f"    - {key}: {value_str}")

        return "\n".join(lines)

    def _make_function_name(self, processor_id: str, processor_name: str) -> str:
        """
        Generate a safe Python function name from processor ID and name.

        Args:
            processor_id: Processor ID
            processor_name: Processor name

        Returns:
            Safe Python function name
        """
        # Try to use processor name first
        name = processor_name.lower()
        # Remove/replace special characters
        name = re.sub(r"[^a-z0-9_]+", "_", name)
        # Remove leading/trailing underscores
        name = name.strip("_")

        # If empty, use processor ID
        if not name:
            name = re.sub(r"[^a-z0-9_]+", "_", processor_id.lower())

        # Ensure it starts with a letter
        if name and not name[0].isalpha():
            name = "process_" + name

        # Ensure uniqueness by appending part of ID
        id_suffix = processor_id.split("-")[-1][:8]
        name = f"{name}_{id_suffix}"

        return name

    def _build_connection_graph(
        self, connections: List[Connection]
    ) -> Dict[str, List[Tuple[str, str]]]:
        """
        Build connection graph from connections list.

        Args:
            connections: List of Connection objects

        Returns:
            Dictionary mapping processor_id -> [(relationship, destination_id), ...]
        """
        graph: Dict[str, List[Tuple[str, str]]] = {}

        for conn in connections:
            if conn.source_id not in graph:
                graph[conn.source_id] = []

            # Add entry for each selected relationship
            for rel in conn.selected_relationships:
                graph[conn.source_id].append((rel, conn.destination_id))

        return graph

    def _get_helper_functions(self) -> List[str]:
        """
        Get helper functions needed for Expression Language support.

        Returns:
            List of helper function code strings
        """
        # For now, return empty list - will be populated when EL transpiler is integrated
        return []

    def register_converter(
        self, processor_type: str, converter_func: callable
    ) -> None:
        """
        Register a processor converter function.

        Args:
            processor_type: Simple processor type name (e.g., "UpdateAttribute")
            converter_func: Converter function with signature:
                (processor: Processor, func_name: str) -> Tuple[str, bool, Set[str], List[str]]
                Returns: (function_code, is_stub, dependencies, warnings)
        """
        self.converter_registry[processor_type] = converter_func


def generate_from_template(template_path: Path, output_format: str = "module") -> GenerationResult:
    """
    Convenience function to generate code from a template.

    Args:
        template_path: Path to NiFi template XML file
        output_format: Output format ("module", "notebook", or "package")

    Returns:
        GenerationResult with generated code

    Example:
        >>> result = generate_from_template(Path("flow.xml"))
        >>> result.save(Path("output.py"))
    """
    generator = CodeGenerator(output_format=output_format)
    return generator.generate_from_template(template_path)
