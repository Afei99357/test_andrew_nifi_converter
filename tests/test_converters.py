"""
Comprehensive test suite for NiFi processor converters.

Tests each converter individually and validates that generated code
is valid Python and executes correctly.
"""

import pytest
from nifi2py.models import Processor, FlowFile, Relationship, Position
from nifi2py.converters import (
    convert_processor,
    get_converter,
    get_registered_types,
    get_converter_coverage,
)


class TestConverterRegistry:
    """Test the converter registry system."""

    def test_get_registered_types(self):
        """Test that converters are registered."""
        registered = get_registered_types()
        assert len(registered) > 0

        # Check specific converters are registered
        assert "org.apache.nifi.processors.attributes.UpdateAttribute" in registered
        assert "org.apache.nifi.processors.standard.LogMessage" in registered
        assert "org.apache.nifi.processors.standard.GenerateFlowFile" in registered
        assert "org.apache.nifi.processors.standard.InvokeHTTP" in registered
        assert "org.apache.nifi.processors.standard.HashContent" in registered
        assert "org.apache.nifi.processors.standard.RouteOnAttribute" in registered

    def test_get_converter(self):
        """Test getting a converter by processor type."""
        converter = get_converter("org.apache.nifi.processors.attributes.UpdateAttribute")
        assert converter is not None

        # Test unknown processor type
        unknown_converter = get_converter("org.apache.nifi.processors.UnknownProcessor")
        assert unknown_converter is None

    def test_converter_coverage(self):
        """Test converter coverage calculation."""
        processors = [
            Processor(
                id="p1",
                name="Test",
                type="org.apache.nifi.processors.attributes.UpdateAttribute",
                relationships=[Relationship(name="success")]
            ),
            Processor(
                id="p2",
                name="Test2",
                type="org.apache.nifi.processors.unknown.Unknown",
                relationships=[Relationship(name="success")]
            ),
        ]

        coverage = get_converter_coverage(processors)
        assert coverage["total"] == 2
        assert coverage["converted"] == 1
        assert coverage["stubbed"] == 1
        assert coverage["coverage_percentage"] == 50.0


class TestUpdateAttributeConverter:
    """Test UpdateAttribute converter."""

    def test_simple_attribute_update(self):
        """Test converting UpdateAttribute with simple static values."""
        processor = Processor(
            id="update-1",
            name="Set Attributes",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={
                "filename": "test.txt",
                "mime.type": "text/plain",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert result.function_name.startswith("process_update_attribute_")
        assert "def " + result.function_name in result.function_code
        assert "filename" in result.function_code
        assert "test.txt" in result.function_code
        assert "mime.type" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')

    def test_attribute_update_with_el(self):
        """Test converting UpdateAttribute with EL expressions."""
        processor = Processor(
            id="update-2",
            name="Set Timestamp",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={
                "timestamp": "${now():format('yyyy-MM-dd')}",
                "q": "nifi",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert "datetime" in result.dependencies
        assert "strftime" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')

    def test_execute_update_attribute(self):
        """Test executing generated UpdateAttribute code."""
        processor = Processor(
            id="update-3",
            name="Set Static",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={
                "test_attr": "test_value",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        # Create test environment
        namespace = {}
        exec(result.function_code, namespace)

        # Get the function
        func = namespace[result.function_name]

        # Create test flowfile
        flowfile = FlowFile(content=b"test", attributes={})

        # Execute function
        output = func(flowfile)

        assert "success" in output
        assert len(output["success"]) == 1
        assert output["success"][0].attributes["test_attr"] == "test_value"


class TestRouteOnAttributeConverter:
    """Test RouteOnAttribute converter."""

    def test_route_on_attribute_equals(self):
        """Test converting RouteOnAttribute with equals condition."""
        processor = Processor(
            id="route-1",
            name="Route On Status",
            type="org.apache.nifi.processors.standard.RouteOnAttribute",
            properties={
                "Routing Strategy": "Route to Property name",
                "200": "${invokehttp.status.code:equals(200)}",
            },
            relationships=[
                Relationship(name="200"),
                Relationship(name="unmatched", auto_terminate=True)
            ]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert "if " in result.function_code
        assert "200" in result.function_code
        assert "invokehttp.status.code" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')

    def test_execute_route_on_attribute(self):
        """Test executing generated RouteOnAttribute code."""
        processor = Processor(
            id="route-2",
            name="Route Test",
            type="org.apache.nifi.processors.standard.RouteOnAttribute",
            properties={
                "200": "${status:equals(200)}",
            },
            relationships=[
                Relationship(name="200"),
                Relationship(name="unmatched")
            ]
        )

        result = convert_processor(processor)

        # Create test environment
        namespace = {}
        exec(result.function_code, namespace)

        # Get the function
        func = namespace[result.function_name]

        # Test matching case
        flowfile = FlowFile(content=b"test", attributes={"status": "200"})
        output = func(flowfile)
        assert "200" in output

        # Test non-matching case
        flowfile2 = FlowFile(content=b"test", attributes={"status": "404"})
        output2 = func(flowfile2)
        assert "unmatched" in output2


class TestLogMessageConverter:
    """Test LogMessage converter."""

    def test_log_message_simple(self):
        """Test converting LogMessage with simple message."""
        processor = Processor(
            id="log-1",
            name="Log",
            type="org.apache.nifi.processors.standard.LogMessage",
            properties={
                "log-level": "INFO",
                "log-message": "Processing file",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert "logging" in result.dependencies
        assert "logger.info" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')


class TestGenerateFlowFileConverter:
    """Test GenerateFlowFile converter."""

    def test_generate_flowfile_custom_text(self):
        """Test converting GenerateFlowFile with custom text."""
        processor = Processor(
            id="gen-1",
            name="Generate",
            type="org.apache.nifi.processors.standard.GenerateFlowFile",
            properties={
                "File Size": "10 b",
                "Custom Text": "test data",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert "test data" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')

    def test_execute_generate_flowfile(self):
        """Test executing generated GenerateFlowFile code."""
        processor = Processor(
            id="gen-2",
            name="Generate",
            type="org.apache.nifi.processors.standard.GenerateFlowFile",
            properties={
                "File Size": "10 b",
                "Batch Size": "1",
                "Custom Text": "hello",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        # Create test environment
        namespace = {}
        exec(result.function_code, namespace)

        # Get the function (no input flowfile)
        func = namespace[result.function_name]

        # Execute function
        output = func()

        assert "success" in output
        assert len(output["success"]) == 1
        assert output["success"][0].content == b"hello"


class TestHashContentConverter:
    """Test HashContent converter."""

    def test_hash_content_md5(self):
        """Test converting HashContent with MD5."""
        processor = Processor(
            id="hash-1",
            name="Hash",
            type="org.apache.nifi.processors.standard.HashContent",
            properties={
                "Hash Attribute Name": "hash.value",
                "Hash Algorithm": "MD5",
            },
            relationships=[
                Relationship(name="success"),
                Relationship(name="failure")
            ]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert "hashlib" in result.dependencies
        assert "md5" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')

    def test_execute_hash_content(self):
        """Test executing generated HashContent code."""
        processor = Processor(
            id="hash-2",
            name="Hash",
            type="org.apache.nifi.processors.standard.HashContent",
            properties={
                "Hash Attribute Name": "content.hash",
                "Hash Algorithm": "SHA-256",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        # Create test environment
        namespace = {}
        exec(result.function_code, namespace)

        # Get the function
        func = namespace[result.function_name]

        # Create test flowfile
        flowfile = FlowFile(content=b"test data", attributes={})

        # Execute function
        output = func(flowfile)

        assert "success" in output
        assert "content.hash" in output["success"][0].attributes
        # SHA-256 hash is 64 chars hex
        assert len(output["success"][0].attributes["content.hash"]) == 64


class TestInvokeHTTPConverter:
    """Test InvokeHTTP converter."""

    def test_invoke_http_get(self):
        """Test converting InvokeHTTP with GET request."""
        processor = Processor(
            id="http-1",
            name="Search Google",
            type="org.apache.nifi.processors.standard.InvokeHTTP",
            properties={
                "HTTP Method": "GET",
                "Remote URL": "http://www.google.com/search?q=${q}",
            },
            relationships=[
                Relationship(name="Original"),
                Relationship(name="Response"),
                Relationship(name="Retry"),
                Relationship(name="Failure"),
                Relationship(name="No Retry")
            ]
        )

        result = convert_processor(processor)

        assert not result.is_stub
        assert "requests" in result.dependencies
        assert "GET" in result.function_code
        assert "google.com" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')


class TestStubConverter:
    """Test stub converter for unsupported processors."""

    def test_stub_conversion(self):
        """Test that unsupported processors generate stubs."""
        processor = Processor(
            id="unknown-1",
            name="Unknown",
            type="org.apache.nifi.processors.unknown.UnknownProcessor",
            properties={
                "some.property": "value",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        assert result.is_stub
        assert result.coverage_percentage == 0
        assert "TODO" in result.function_code
        assert "NotImplementedError" in result.function_code
        assert "some.property" in result.function_code

        # Test code compiles
        compile(result.function_code, '<string>', 'exec')

    def test_stub_migration_hints(self):
        """Test that stub converter provides migration hints."""
        processor = Processor(
            id="exec-1",
            name="Impala Query",
            type="org.apache.nifi.processors.standard.ExecuteStreamCommand",
            properties={
                "Command Path": "/bin/impala-shell",
                "Command Arguments": "-q \"SELECT * FROM table\"",
            },
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        assert result.is_stub
        # Should contain migration hints
        assert "impala" in result.function_code.lower() or "spark" in result.function_code.lower()


class TestCodeGeneration:
    """Test code generation quality."""

    def test_all_generated_code_compiles(self):
        """Test that all converter types generate valid Python code."""
        processor_types = [
            ("org.apache.nifi.processors.attributes.UpdateAttribute", {"attr": "value"}),
            ("org.apache.nifi.processors.standard.RouteOnAttribute", {"route": "${attr:equals('x')}"}),
            ("org.apache.nifi.processors.standard.LogMessage", {"log-message": "test"}),
            ("org.apache.nifi.processors.standard.GenerateFlowFile", {"Custom Text": "test"}),
            ("org.apache.nifi.processors.standard.HashContent", {"Hash Algorithm": "MD5"}),
            ("org.apache.nifi.processors.standard.InvokeHTTP", {"Remote URL": "http://test.com"}),
        ]

        for proc_type, properties in processor_types:
            processor = Processor(
                id="test-" + proc_type.split(".")[-1],
                name="Test",
                type=proc_type,
                properties=properties,
                relationships=[Relationship(name="success")]
            )

            result = convert_processor(processor)

            # All generated code must compile
            compile(result.function_code, f'<{proc_type}>', 'exec')

    def test_function_names_are_valid(self):
        """Test that generated function names are valid Python identifiers."""
        processor = Processor(
            id="test-123-abc",
            name="Test Processor",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={},
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        # Function name should be valid Python identifier
        assert result.function_name.isidentifier()
        assert result.function_name.startswith("process_")

    def test_docstrings_present(self):
        """Test that generated functions have docstrings."""
        processor = Processor(
            id="test-doc",
            name="Test Doc",
            type="org.apache.nifi.processors.standard.LogMessage",
            properties={},
            relationships=[Relationship(name="success")]
        )

        result = convert_processor(processor)

        # Should contain docstring
        assert '"""' in result.function_code
        assert processor.id in result.function_code
        assert processor.name in result.function_code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
