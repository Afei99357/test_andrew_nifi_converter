"""
Unit tests for template parser
"""

import pytest
from pathlib import Path
from lxml import etree
from nifi2py.template_parser import (
    TemplateParser,
    parse_template,
    analyze_template,
    Processor,
    Connection,
    FlowGraph
)


@pytest.fixture
def example_template_path():
    """Path to the example template file"""
    return Path(__file__).parent.parent / "examples" / "InvokeHttp_And_Route_Original_On_Status.xml"


@pytest.fixture
def parser():
    """Template parser instance"""
    return TemplateParser()


class TestTemplateParser:
    """Tests for TemplateParser class"""

    def test_parse_template_exists(self, parser, example_template_path):
        """Test parsing an existing template"""
        flow_graph = parser.parse_template(example_template_path)
        assert flow_graph is not None
        assert isinstance(flow_graph, FlowGraph)

    def test_parse_template_missing_file(self, parser):
        """Test parsing a non-existent file raises FileNotFoundError"""
        with pytest.raises(FileNotFoundError):
            parser.parse_template(Path("/nonexistent/template.xml"))

    def test_parse_template_metadata(self, parser, example_template_path):
        """Test extraction of template metadata"""
        flow_graph = parser.parse_template(example_template_path)
        assert flow_graph.template_name == "InvokeHttp And Route Original On Status"
        assert flow_graph.timestamp == "12/18/2015 15:52:29 EST"
        assert "HTTP service" in flow_graph.template_description

    def test_parse_processors(self, parser, example_template_path):
        """Test processor extraction"""
        flow_graph = parser.parse_template(example_template_path)
        assert len(flow_graph.processors) == 5

        # Check processor types
        processor_types = [p.get_short_type() for p in flow_graph.processors.values()]
        assert "UpdateAttribute" in processor_types
        assert "GenerateFlowFile" in processor_types
        assert "InvokeHTTP" in processor_types
        assert "RouteOnAttribute" in processor_types
        assert "HashContent" in processor_types

    def test_parse_connections(self, parser, example_template_path):
        """Test connection extraction"""
        flow_graph = parser.parse_template(example_template_path)
        assert len(flow_graph.connections) == 5

        # Check that all connections have source and destination
        for conn in flow_graph.connections:
            assert conn.source_id
            assert conn.destination_id
            assert len(conn.relationships) > 0

    def test_processor_properties(self, parser, example_template_path):
        """Test processor property extraction"""
        flow_graph = parser.parse_template(example_template_path)

        # Find the UpdateAttribute processor
        update_attr = None
        for proc in flow_graph.processors.values():
            if proc.get_short_type() == "UpdateAttribute":
                update_attr = proc
                break

        assert update_attr is not None
        assert "q" in update_attr.properties
        assert update_attr.properties["q"] == "nifi"

    def test_processor_relationships(self, parser, example_template_path):
        """Test processor relationship extraction"""
        flow_graph = parser.parse_template(example_template_path)

        # Find the InvokeHTTP processor
        invoke_http = None
        for proc in flow_graph.processors.values():
            if proc.get_short_type() == "InvokeHTTP":
                invoke_http = proc
                break

        assert invoke_http is not None
        assert "Original" in invoke_http.relationships
        assert "Failure" in invoke_http.relationships
        assert "Response" in invoke_http.relationships
        assert invoke_http.relationships["Response"]  # auto-terminated

    def test_el_expression_extraction(self, parser, example_template_path):
        """Test EL expression extraction"""
        flow_graph = parser.parse_template(example_template_path)
        expressions = parser.extract_el_expressions(flow_graph)

        assert len(expressions) == 2

        # Check for specific expressions
        el_values = [expr[3] for expr in expressions]  # Get the expression values
        assert any("${q}" in val for val in el_values)
        assert any("${invokehttp.status.code:equals(200)}" in val for val in el_values)

    def test_el_function_detection(self, parser, example_template_path):
        """Test detection of EL functions"""
        flow_graph = parser.parse_template(example_template_path)
        expressions = parser.extract_el_expressions(flow_graph)

        # Extract all expression values
        expr_values = [expr[3] for expr in expressions]
        combined = " ".join(expr_values)

        # Check for equals function
        assert ":equals(" in combined

    def test_get_processor_by_id(self, parser, example_template_path):
        """Test getting processor by ID"""
        flow_graph = parser.parse_template(example_template_path)

        # Get a known processor ID
        first_proc_id = list(flow_graph.processors.keys())[0]
        processor = flow_graph.get_processor_by_id(first_proc_id)

        assert processor is not None
        assert processor.id == first_proc_id

    def test_get_processor_by_id_missing(self, parser, example_template_path):
        """Test getting non-existent processor returns None"""
        flow_graph = parser.parse_template(example_template_path)
        processor = flow_graph.get_processor_by_id("nonexistent-id")
        assert processor is None

    def test_get_outgoing_connections(self, parser, example_template_path):
        """Test getting outgoing connections for a processor"""
        flow_graph = parser.parse_template(example_template_path)

        # Find GenerateFlowFile processor
        gen_proc = None
        for proc in flow_graph.processors.values():
            if proc.get_short_type() == "GenerateFlowFile":
                gen_proc = proc
                break

        assert gen_proc is not None

        outgoing = flow_graph.get_outgoing_connections(gen_proc.id)
        assert len(outgoing) >= 1

    def test_get_incoming_connections(self, parser, example_template_path):
        """Test getting incoming connections for a processor"""
        flow_graph = parser.parse_template(example_template_path)

        # Find HashContent processor (should have incoming connections)
        hash_proc = None
        for proc in flow_graph.processors.values():
            if proc.get_short_type() == "HashContent":
                hash_proc = proc
                break

        assert hash_proc is not None

        incoming = flow_graph.get_incoming_connections(hash_proc.id)
        assert len(incoming) >= 1


class TestAnalyzeTemplate:
    """Tests for template analysis function"""

    def test_analyze_template(self, example_template_path):
        """Test complete template analysis"""
        analysis = analyze_template(example_template_path)

        assert analysis['total_processors'] == 5
        assert analysis['total_connections'] == 5
        assert analysis['el_expression_count'] == 2
        assert len(analysis['unique_processor_types']) == 5
        assert 'equals' in analysis['unique_el_functions']

    def test_analyze_processor_type_counts(self, example_template_path):
        """Test processor type counting"""
        analysis = analyze_template(example_template_path)

        proc_types = analysis['processor_types']
        assert proc_types['UpdateAttribute'] == 1
        assert proc_types['GenerateFlowFile'] == 1
        assert proc_types['InvokeHTTP'] == 1
        assert proc_types['RouteOnAttribute'] == 1
        assert proc_types['HashContent'] == 1


class TestConvenienceFunctions:
    """Tests for convenience functions"""

    def test_parse_template_function(self, example_template_path):
        """Test parse_template convenience function"""
        flow_graph = parse_template(example_template_path)
        assert flow_graph is not None
        assert len(flow_graph.processors) > 0

    def test_analyze_template_function(self, example_template_path):
        """Test analyze_template convenience function"""
        analysis = analyze_template(example_template_path)
        assert 'total_processors' in analysis
        assert 'flow_graph' in analysis


class TestProcessorModel:
    """Tests for Processor data model"""

    def test_processor_creation(self):
        """Test creating a Processor"""
        proc = Processor(
            id="test-123",
            name="Test Processor",
            type="org.apache.nifi.processors.test.TestProcessor",
            parent_group_id="group-1"
        )
        assert proc.id == "test-123"
        assert proc.get_short_type() == "TestProcessor"

    def test_processor_with_properties(self):
        """Test Processor with properties"""
        proc = Processor(
            id="test-123",
            name="Test",
            type="org.apache.nifi.processors.test.Test",
            parent_group_id="group-1",
            properties={"key1": "value1", "key2": None}
        )
        assert "key1" in proc.properties
        assert proc.properties["key1"] == "value1"
        assert proc.properties["key2"] is None


class TestConnectionModel:
    """Tests for Connection data model"""

    def test_connection_creation(self):
        """Test creating a Connection"""
        conn = Connection(
            id="conn-123",
            source_id="proc-1",
            source_type="PROCESSOR",
            destination_id="proc-2",
            destination_type="PROCESSOR",
            relationships=["success"]
        )
        assert conn.id == "conn-123"
        assert conn.source_id == "proc-1"
        assert "success" in conn.relationships


class TestFlowGraphModel:
    """Tests for FlowGraph data model"""

    def test_flow_graph_creation(self):
        """Test creating a FlowGraph"""
        flow = FlowGraph()
        assert len(flow.processors) == 0
        assert len(flow.connections) == 0

    def test_flow_graph_with_processors(self):
        """Test FlowGraph with processors"""
        proc1 = Processor(id="p1", name="P1", type="T1", parent_group_id="g1")
        proc2 = Processor(id="p2", name="P2", type="T2", parent_group_id="g1")

        flow = FlowGraph(processors={"p1": proc1, "p2": proc2})
        assert len(flow.processors) == 2
        assert flow.get_processor_by_id("p1") == proc1


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_empty_properties(self, parser):
        """Test processor with empty property values"""
        # Create minimal XML with empty property
        xml_str = """<?xml version="1.0"?>
        <template>
            <snippet>
                <processors>
                    <id>test-1</id>
                    <name>Test</name>
                    <type>org.apache.nifi.processors.test.Test</type>
                    <config>
                        <properties>
                            <entry>
                                <key>EmptyProp</key>
                            </entry>
                        </properties>
                    </config>
                </processors>
            </snippet>
        </template>"""

        # Parse from string
        root = etree.fromstring(xml_str.encode())
        snippet = root.find('snippet')
        proc_elem = snippet.find('processors')

        processor = parser.extract_processor_info(proc_elem)
        assert "EmptyProp" in processor.properties
        assert processor.properties["EmptyProp"] is None

    def test_processor_without_config(self, parser):
        """Test processor without config element"""
        xml_str = """<?xml version="1.0"?>
        <template>
            <snippet>
                <processors>
                    <id>test-1</id>
                    <name>Test</name>
                    <type>org.apache.nifi.processors.test.Test</type>
                </processors>
            </snippet>
        </template>"""

        root = etree.fromstring(xml_str.encode())
        snippet = root.find('snippet')
        proc_elem = snippet.find('processors')

        processor = parser.extract_processor_info(proc_elem)
        assert len(processor.properties) == 0
        assert len(processor.relationships) == 0

    def test_multiple_selected_relationships(self, parser):
        """Test connection with multiple selected relationships"""
        xml_str = """<?xml version="1.0"?>
        <template>
            <snippet>
                <connections>
                    <id>conn-1</id>
                    <source>
                        <id>source-1</id>
                        <type>PROCESSOR</type>
                    </source>
                    <destination>
                        <id>dest-1</id>
                        <type>PROCESSOR</type>
                    </destination>
                    <selectedRelationships>success</selectedRelationships>
                    <selectedRelationships>failure</selectedRelationships>
                </connections>
            </snippet>
        </template>"""

        root = etree.fromstring(xml_str.encode())
        snippet = root.find('snippet')
        conn_elem = snippet.find('connections')

        connection = parser.extract_connection_info(conn_elem)
        assert len(connection.relationships) == 2
        assert "success" in connection.relationships
        assert "failure" in connection.relationships


class TestELPatternMatching:
    """Tests for EL pattern matching"""

    def test_simple_el_pattern(self, parser):
        """Test simple EL expression detection"""
        text = "Value is ${attribute}"
        assert parser.EL_PATTERN.search(text)

    def test_el_with_functions(self, parser):
        """Test EL with function calls"""
        text = "${filename:substring(0,5):toUpper()}"
        assert parser.EL_PATTERN.search(text)

    def test_multiple_el_expressions(self, parser):
        """Test multiple EL expressions in one string"""
        text = "File: ${filename} at ${timestamp}"
        matches = parser.EL_PATTERN.findall(text)
        assert len(matches) == 2

    def test_no_el_pattern(self, parser):
        """Test text without EL expressions"""
        text = "Regular text without expressions"
        assert not parser.EL_PATTERN.search(text)
