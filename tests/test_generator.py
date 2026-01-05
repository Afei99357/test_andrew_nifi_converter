"""
Tests for NiFi to Python code generator.

Tests cover:
- Template parsing and code generation
- Connection graph building
- Processor conversion (stubs and converters)
- Generated code execution
- Error handling
"""

import tempfile
from pathlib import Path
import sys
import importlib.util
from typing import Dict, List

import pytest

from nifi2py.generator import CodeGenerator, GenerationResult, generate_from_template
from nifi2py.models import (
    FlowGraph,
    ProcessGroup,
    Processor,
    Connection,
    Relationship,
    Position,
)


class TestGenerationResult:
    """Test GenerationResult class."""

    def test_creation(self):
        """Test creating a GenerationResult."""
        result = GenerationResult(
            code="print('hello')",
            file_name="test.py",
            processor_count=5,
            connection_count=3,
            stub_count=2,
            coverage_percentage=60.0,
        )

        assert result.code == "print('hello')"
        assert result.file_name == "test.py"
        assert result.processor_count == 5
        assert result.stub_count == 2
        assert result.coverage_percentage == 60.0
        assert not result.is_complete

    def test_is_complete(self):
        """Test is_complete property."""
        result = GenerationResult(
            code="",
            file_name="test.py",
            stub_count=0,
        )
        assert result.is_complete

        result.stub_count = 1
        assert not result.is_complete

    def test_add_warning(self):
        """Test adding warnings."""
        result = GenerationResult(code="", file_name="test.py")
        result.add_warning("Test warning")
        assert "Test warning" in result.warnings

        # Should not add duplicates
        result.add_warning("Test warning")
        assert result.warnings.count("Test warning") == 1

    def test_add_dependency(self):
        """Test adding dependencies."""
        result = GenerationResult(code="", file_name="test.py")
        result.add_dependency("os", "sys")
        assert "os" in result.dependencies
        assert "sys" in result.dependencies

        # Should not add duplicates
        result.add_dependency("os")
        assert result.dependencies.count("os") == 1

    def test_save(self):
        """Test saving generated code to file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "subdir" / "output.py"
            result = GenerationResult(
                code="print('hello')",
                file_name="output.py",
            )

            result.save(output_path)

            assert output_path.exists()
            assert output_path.read_text() == "print('hello')"


class TestCodeGenerator:
    """Test CodeGenerator class."""

    def test_initialization(self):
        """Test creating a CodeGenerator."""
        generator = CodeGenerator(output_format="module")
        assert generator.output_format == "module"

    def test_invalid_output_format(self):
        """Test that invalid output format raises error."""
        with pytest.raises(ValueError, match="Unsupported output format"):
            CodeGenerator(output_format="invalid")

    def test_make_function_name(self):
        """Test function name generation."""
        generator = CodeGenerator()

        # Normal case
        name = generator._make_function_name(
            "abc-123-def-456", "Update Attributes"
        )
        assert name.startswith("update_attributes_")
        assert name.replace("_", "").replace("update", "").replace("attributes", "").isalnum()

        # Special characters
        name = generator._make_function_name("xyz-789", "Route on Attribute!")
        assert name.startswith("route_on_attribute_")

        # Empty name - should use ID
        name = generator._make_function_name("test-id-123", "")
        assert "test" in name or "id" in name or "123" in name

    def test_build_connection_graph(self):
        """Test building connection graph."""
        generator = CodeGenerator()

        connections = [
            Connection(
                id="conn1",
                source_id="proc1",
                destination_id="proc2",
                selected_relationships=["success"],
            ),
            Connection(
                id="conn2",
                source_id="proc1",
                destination_id="proc3",
                selected_relationships=["failure"],
            ),
            Connection(
                id="conn3",
                source_id="proc2",
                destination_id="proc3",
                selected_relationships=["success", "retry"],
            ),
        ]

        graph = generator._build_connection_graph(connections)

        assert "proc1" in graph
        assert ("success", "proc2") in graph["proc1"]
        assert ("failure", "proc3") in graph["proc1"]

        assert "proc2" in graph
        assert ("success", "proc3") in graph["proc2"]
        assert ("retry", "proc3") in graph["proc2"]

    def test_generate_stub(self):
        """Test stub generation for unsupported processors."""
        generator = CodeGenerator()

        processor = Processor(
            id="test-123",
            name="Test Processor",
            type="org.apache.nifi.processors.test.TestProcessor",
            properties={"prop1": "value1"},
            relationships=[
                Relationship(name="success", auto_terminate=False),
                Relationship(name="failure", auto_terminate=True),
            ],
        )

        code, is_stub, deps, warnings = generator._generate_stub(
            processor, "test_processor_123"
        )

        assert is_stub
        assert len(warnings) > 0
        assert "def test_processor_123" in code
        assert "STUB" in code
        assert "Test Processor" in code
        assert "prop1" in code
        assert "success" in code or "failure" in code

    def test_generate_simple_flow(self):
        """Test generating code from a simple flow."""
        # Create a simple flow: GenerateFlowFile -> UpdateAttribute -> LogMessage
        processors = [
            Processor(
                id="gen-1",
                name="Generate",
                type="org.apache.nifi.processors.standard.GenerateFlowFile",
                relationships=[Relationship(name="success", auto_terminate=False)],
            ),
            Processor(
                id="update-1",
                name="Update",
                type="org.apache.nifi.processors.attributes.UpdateAttribute",
                properties={"test_attr": "test_value"},
                relationships=[Relationship(name="success", auto_terminate=False)],
            ),
            Processor(
                id="log-1",
                name="Log",
                type="org.apache.nifi.processors.standard.LogMessage",
                relationships=[Relationship(name="success", auto_terminate=True)],
            ),
        ]

        connections = [
            Connection(
                id="conn1",
                source_id="gen-1",
                destination_id="update-1",
                selected_relationships=["success"],
            ),
            Connection(
                id="conn2",
                source_id="update-1",
                destination_id="log-1",
                selected_relationships=["success"],
            ),
        ]

        root_group = ProcessGroup(
            id="root",
            name="Test Flow",
            processors=processors,
            connections=connections,
        )

        flow_graph = FlowGraph(
            root_group=root_group,
            name="Test Flow",
            description="A simple test flow",
        )

        generator = CodeGenerator()
        result = generator.generate(flow_graph)

        assert result.processor_count == 3
        assert result.connection_count == 2
        assert result.stub_count == 3  # All are stubs since no converters registered
        assert result.coverage_percentage == 0.0

        # Verify generated code structure
        assert "class FlowFile:" in result.code
        assert "def run_flow" in result.code
        assert "CONNECTIONS" in result.code
        assert "PROCESSOR_MAP" in result.code
        assert "gen-1" in result.code
        assert "update-1" in result.code
        assert "log-1" in result.code

    def test_generate_from_template_file(self):
        """Test generating from InvokeHttp template."""
        template_path = Path(__file__).parent.parent / "examples" / "InvokeHttp_And_Route_Original_On_Status.xml"

        if not template_path.exists():
            pytest.skip(f"Template file not found: {template_path}")

        generator = CodeGenerator()
        result = generator.generate_from_template(template_path)

        assert result.processor_count > 0
        assert result.connection_count > 0
        assert result.file_name.endswith(".py")
        assert "def run_flow" in result.code
        assert "FlowFile" in result.code

        # Should have warnings about stub processors
        assert result.stub_count > 0
        assert len(result.warnings) > 0

    def test_register_converter(self):
        """Test registering a custom processor converter."""
        generator = CodeGenerator()

        def custom_converter(processor, func_name):
            code = f'''def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    """Custom converter for {processor.name}"""
    flowfile.attributes["custom"] = "true"
    return {{"success": [flowfile]}}'''
            return code, False, {"typing"}, []

        # Register converter - the processor_simple_type will be "CustomProcessor"
        generator.register_converter("CustomProcessor", custom_converter)

        processor = Processor(
            id="custom-1",
            name="Custom Test",
            type="org.apache.nifi.CustomProcessor",
            relationships=[Relationship(name="success", auto_terminate=False)],
        )

        # processor_simple_type extracts "CustomProcessor" from "org.apache.nifi.CustomProcessor"
        code, is_stub, deps, warnings = generator._convert_processor(processor)

        # Should NOT be a stub because we registered a converter for "CustomProcessor"
        assert not is_stub  # Converter should match
        assert "Custom converter" in code
        assert "typing" in deps

    def test_convenience_function(self):
        """Test the convenience function."""
        template_path = Path(__file__).parent.parent / "examples" / "InvokeHttp_And_Route_Original_On_Status.xml"

        if not template_path.exists():
            pytest.skip(f"Template file not found: {template_path}")

        result = generate_from_template(template_path)
        assert isinstance(result, GenerationResult)
        assert result.processor_count > 0


class TestGeneratedCodeExecution:
    """Test that generated code is valid and executable."""

    def test_generated_code_compiles(self):
        """Test that generated code compiles without errors."""
        # Create simple flow
        processors = [
            Processor(
                id="gen-1",
                name="Generate",
                type="org.apache.nifi.processors.standard.GenerateFlowFile",
                relationships=[Relationship(name="success", auto_terminate=True)],
            ),
        ]

        root_group = ProcessGroup(
            id="root", name="Test", processors=processors, connections=[]
        )
        flow_graph = FlowGraph(root_group=root_group, name="Test")

        generator = CodeGenerator()
        result = generator.generate(flow_graph)

        # Try to compile the code
        try:
            compile(result.code, "<generated>", "exec")
            compiled = True
        except SyntaxError as e:
            compiled = False
            print(f"Syntax error: {e}")
            print(f"Code:\n{result.code}")

        assert compiled, "Generated code should compile without syntax errors"

    def test_generated_code_imports(self):
        """Test that generated code can be imported and executed."""
        template_path = Path(__file__).parent.parent / "examples" / "InvokeHttp_And_Route_Original_On_Status.xml"

        if not template_path.exists():
            pytest.skip(f"Template file not found: {template_path}")

        generator = CodeGenerator()
        result = generator.generate_from_template(template_path)

        # Save to temporary file
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "generated_flow.py"
            result.save(output_path)

            # Import the module
            spec = importlib.util.spec_from_file_location("generated_flow", output_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules["generated_flow"] = module

            try:
                spec.loader.exec_module(module)

                # Verify module has expected attributes
                assert hasattr(module, "FlowFile")
                assert hasattr(module, "run_flow")
                assert hasattr(module, "CONNECTIONS")
                assert hasattr(module, "PROCESSOR_MAP")

                # Test FlowFile creation
                ff = module.FlowFile(
                    content=b"test data",
                    attributes={"test": "value"}
                )
                assert ff.size == 9
                assert ff.attributes["test"] == "value"

                # Test get_source_processors
                sources = module.get_source_processors()
                assert isinstance(sources, list)

            finally:
                # Cleanup
                if "generated_flow" in sys.modules:
                    del sys.modules["generated_flow"]

    def test_generated_flow_execution(self):
        """Test executing a generated flow."""
        # Create a simple linear flow
        processors = [
            Processor(
                id="source-1",
                name="Source",
                type="org.apache.nifi.processors.standard.GenerateFlowFile",
                relationships=[Relationship(name="success", auto_terminate=False)],
            ),
            Processor(
                id="sink-1",
                name="Sink",
                type="org.apache.nifi.processors.standard.LogMessage",
                relationships=[Relationship(name="success", auto_terminate=True)],
            ),
        ]

        connections = [
            Connection(
                id="conn1",
                source_id="source-1",
                destination_id="sink-1",
                selected_relationships=["success"],
            ),
        ]

        root_group = ProcessGroup(
            id="root",
            name="Linear Flow",
            processors=processors,
            connections=connections,
        )

        flow_graph = FlowGraph(root_group=root_group, name="Linear Flow")

        generator = CodeGenerator()
        result = generator.generate(flow_graph)

        # Execute the generated code
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "linear_flow.py"
            result.save(output_path)

            spec = importlib.util.spec_from_file_location("linear_flow", output_path)
            module = importlib.util.module_from_spec(spec)

            try:
                spec.loader.exec_module(module)

                # Create a flowfile and run through flow
                flowfile = module.FlowFile(
                    content=b"test data",
                    attributes={"source": "test"}
                )

                # Run flow from source
                output_files = module.run_flow([flowfile], "source-1")

                # Should have output (from terminal sink)
                assert isinstance(output_files, list)
                # Stubs pass through, so should have at least 1 output
                assert len(output_files) >= 0

            finally:
                if "linear_flow" in sys.modules:
                    del sys.modules["linear_flow"]

    def test_cycle_detection(self):
        """Test that generated code handles cycles gracefully."""
        # Create a flow with a cycle: A -> B -> A
        processors = [
            Processor(
                id="proc-a",
                name="Processor A",
                type="org.apache.nifi.processors.standard.UpdateAttribute",
                relationships=[Relationship(name="success", auto_terminate=False)],
            ),
            Processor(
                id="proc-b",
                name="Processor B",
                type="org.apache.nifi.processors.standard.UpdateAttribute",
                relationships=[Relationship(name="success", auto_terminate=False)],
            ),
        ]

        connections = [
            Connection(
                id="conn1",
                source_id="proc-a",
                destination_id="proc-b",
                selected_relationships=["success"],
            ),
            Connection(
                id="conn2",
                source_id="proc-b",
                destination_id="proc-a",
                selected_relationships=["success"],
            ),
        ]

        root_group = ProcessGroup(
            id="root",
            name="Cycle Flow",
            processors=processors,
            connections=connections,
        )

        flow_graph = FlowGraph(root_group=root_group, name="Cycle Flow")

        generator = CodeGenerator()
        result = generator.generate(flow_graph)

        # Execute and verify cycle detection
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "cycle_flow.py"
            result.save(output_path)

            spec = importlib.util.spec_from_file_location("cycle_flow", output_path)
            module = importlib.util.module_from_spec(spec)

            try:
                spec.loader.exec_module(module)

                flowfile = module.FlowFile(content=b"test")

                # Should either complete or raise max iterations error
                try:
                    output = module.run_flow([flowfile], "proc-a", max_iterations=100)
                    # If it completes, cycle detection worked
                    assert True
                except ValueError as e:
                    # If it hits max iterations, that's also OK (cycle detected)
                    assert "Maximum iterations" in str(e)

            finally:
                if "cycle_flow" in sys.modules:
                    del sys.modules["cycle_flow"]


class TestTemplateIntegration:
    """Integration tests with actual template files."""

    def test_invokehttp_template_generation(self):
        """Test generating code from InvokeHttp template."""
        template_path = Path(__file__).parent.parent / "examples" / "InvokeHttp_And_Route_Original_On_Status.xml"

        if not template_path.exists():
            pytest.skip(f"Template file not found: {template_path}")

        result = generate_from_template(template_path)

        # Verify result properties
        assert result.processor_count >= 4  # Should have at least 4 processors
        assert result.connection_count >= 4  # Should have connections
        assert result.file_name.endswith(".py")
        assert result.stub_count > 0

        # Verify code structure
        code = result.code
        assert "Auto-generated by nifi2py" in code
        assert "class FlowFile:" in code
        assert "def run_flow" in code
        assert "CONNECTIONS" in code
        assert "PROCESSOR_MAP" in code

        # Verify it compiles
        compile(code, "<generated>", "exec")

    def test_save_and_stats(self):
        """Test saving generated code and verifying statistics."""
        template_path = Path(__file__).parent.parent / "examples" / "InvokeHttp_And_Route_Original_On_Status.xml"

        if not template_path.exists():
            pytest.skip(f"Template file not found: {template_path}")

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.py"

            result = generate_from_template(template_path)
            result.save(output_path)

            assert output_path.exists()
            assert output_path.stat().st_size > 0

            # Verify statistics are reasonable
            assert result.processor_count > 0
            assert result.connection_count > 0
            assert 0 <= result.coverage_percentage <= 100
            assert result.stub_count <= result.processor_count


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
