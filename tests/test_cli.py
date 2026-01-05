"""
Tests for nifi2py CLI

Comprehensive test suite for all CLI commands using Click's CliRunner.
Tests command execution, error handling, and output formatting.
"""

import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
from click.testing import CliRunner

from nifi2py.cli import main, generate_python_stub
from nifi2py.template_parser import FlowGraph, Processor


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def sample_template_path(tmp_path):
    """Create a sample template XML file for testing."""
    template_content = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<template encoding-version="1.3">
    <description>Test flow</description>
    <groupId>test-group</groupId>
    <name>Test Flow</name>
    <snippet>
        <processors>
            <id>test-1</id>
            <parentGroupId>test-group</parentGroupId>
            <position><x>100</x><y>100</y></position>
            <name>Test Processor</name>
            <type>org.apache.nifi.processors.standard.LogMessage</type>
            <config>
                <properties>
                    <entry><key>log-level</key><value>INFO</value></entry>
                </properties>
            </config>
            <state>RUNNING</state>
        </processors>
    </snippet>
</template>"""

    template_file = tmp_path / "test_flow.xml"
    template_file.write_text(template_content)
    return template_file


class TestMainCommand:
    """Tests for the main CLI entry point."""

    def test_main_help(self, runner):
        """Test that --help shows usage information."""
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "nifi2py - Convert Apache NiFi flows to Python code" in result.output
        assert "parse-template" in result.output
        assert "convert" in result.output
        assert "test-connection" in result.output

    def test_main_version(self, runner):
        """Test that --version shows version information."""
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output


class TestParseTemplate:
    """Tests for the parse-template command."""

    def test_parse_template_basic(self, runner, sample_template_path):
        """Test basic template parsing."""
        result = runner.invoke(main, ["parse-template", str(sample_template_path)])

        assert result.exit_code == 0
        assert "Successfully parsed template" in result.output
        assert "Template Overview" in result.output
        assert "Processor Types" in result.output

    def test_parse_template_with_output(self, runner, sample_template_path, tmp_path):
        """Test parsing template with JSON output."""
        output_file = tmp_path / "output.json"
        result = runner.invoke(
            main,
            ["parse-template", str(sample_template_path), "-o", str(output_file)]
        )

        assert result.exit_code == 0
        assert output_file.exists()

        # Verify JSON content
        data = json.loads(output_file.read_text())
        assert "template_name" in data
        assert "total_processors" in data
        assert "processor_types" in data
        assert data["total_processors"] > 0

    def test_parse_template_verbose(self, runner, sample_template_path):
        """Test parsing with verbose output."""
        result = runner.invoke(
            main,
            ["parse-template", str(sample_template_path), "-v"]
        )

        assert result.exit_code == 0
        assert "Detailed Processor Information" in result.output

    def test_parse_template_show_el(self, runner, sample_template_path):
        """Test parsing with EL expression display."""
        result = runner.invoke(
            main,
            ["parse-template", str(sample_template_path), "--show-el"]
        )

        assert result.exit_code == 0
        # Output may or may not show EL table depending on if expressions exist
        assert result.exit_code == 0

    def test_parse_template_nonexistent_file(self, runner):
        """Test parsing with nonexistent file."""
        result = runner.invoke(
            main,
            ["parse-template", "/nonexistent/file.xml"]
        )

        assert result.exit_code != 0
        assert "Error" in result.output or "does not exist" in result.output

    def test_parse_template_invalid_xml(self, runner, tmp_path):
        """Test parsing with invalid XML."""
        invalid_file = tmp_path / "invalid.xml"
        invalid_file.write_text("not valid xml")

        result = runner.invoke(main, ["parse-template", str(invalid_file)])

        assert result.exit_code != 0
        assert "Error" in result.output


class TestConvert:
    """Tests for the convert command."""

    def test_convert_basic(self, runner, sample_template_path, tmp_path):
        """Test basic template conversion."""
        output_file = tmp_path / "output.py"
        result = runner.invoke(
            main,
            ["convert", str(sample_template_path), "-o", str(output_file)]
        )

        assert result.exit_code == 0
        assert "Conversion complete" in result.output
        assert output_file.exists()

        # Verify generated Python code
        code = output_file.read_text()
        assert "class FlowFile:" in code
        assert "class Flow:" in code

    def test_convert_with_preview(self, runner, sample_template_path, tmp_path):
        """Test conversion with code preview."""
        output_file = tmp_path / "output.py"
        result = runner.invoke(
            main,
            ["convert", str(sample_template_path), "-o", str(output_file), "--show-preview"]
        )

        assert result.exit_code == 0
        assert "Code Preview" in result.output
        assert "Conversion complete" in result.output

    def test_convert_missing_output(self, runner, sample_template_path):
        """Test conversion without output file (should fail)."""
        result = runner.invoke(
            main,
            ["convert", str(sample_template_path)]
        )

        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()

    def test_convert_nonexistent_template(self, runner, tmp_path):
        """Test conversion with nonexistent template."""
        output_file = tmp_path / "output.py"
        result = runner.invoke(
            main,
            ["convert", "/nonexistent/template.xml", "-o", str(output_file)]
        )

        assert result.exit_code != 0
        assert "Error" in result.output or "does not exist" in result.output


class TestTestConnection:
    """Tests for the test-connection command."""

    @patch("nifi2py.cli.NiFiClient")
    def test_test_connection_success(self, mock_client_class, runner):
        """Test successful NiFi connection."""
        # Mock the client
        mock_client = Mock()
        mock_client.get_root_process_group_id.return_value = "root-id-123"
        mock_client.list_processors.return_value = [
            {"id": "1", "name": "Proc1"},
            {"id": "2", "name": "Proc2"}
        ]
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main,
            [
                "test-connection",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "admin",
                "--password", "admin"
            ]
        )

        assert result.exit_code == 0
        assert "Successfully connected" in result.output
        assert "root-id-123" in result.output
        assert "Total Processors" in result.output

    @patch("nifi2py.cli.NiFiClient")
    def test_test_connection_auth_error(self, mock_client_class, runner):
        """Test connection with authentication error."""
        from nifi2py.client import NiFiAuthError

        mock_client_class.side_effect = NiFiAuthError("Invalid credentials")

        result = runner.invoke(
            main,
            [
                "test-connection",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "wrong",
                "--password", "wrong"
            ]
        )

        assert result.exit_code != 0
        assert "Authentication Error" in result.output or "Error" in result.output

    def test_test_connection_missing_url(self, runner):
        """Test connection without URL."""
        result = runner.invoke(
            main,
            ["test-connection", "--user", "admin", "--password", "admin"]
        )

        assert result.exit_code != 0
        assert "Error" in result.output

    @patch.dict("os.environ", {"NIFI_URL": "http://localhost:8080/nifi-api", "NIFI_USER": "admin", "NIFI_PASSWORD": "admin"})
    @patch("nifi2py.cli.NiFiClient")
    def test_test_connection_env_vars(self, mock_client_class, runner):
        """Test connection using environment variables."""
        mock_client = Mock()
        mock_client.get_root_process_group_id.return_value = "root-id-123"
        mock_client.list_processors.return_value = []
        mock_client_class.return_value = mock_client

        result = runner.invoke(main, ["test-connection"])

        assert result.exit_code == 0
        assert "Successfully connected" in result.output


class TestListProcessors:
    """Tests for the list-processors command."""

    @patch("nifi2py.cli.NiFiClient")
    def test_list_processors_basic(self, mock_client_class, runner):
        """Test basic processor listing."""
        mock_client = Mock()
        mock_client.get_root_process_group_id.return_value = "root-id"
        mock_client.list_processors.return_value = [
            {
                "id": "1",
                "component": {"name": "Processor1", "type": "org.apache.nifi.processors.standard.LogMessage"},
                "status": {"runStatus": "RUNNING"}
            },
            {
                "id": "2",
                "component": {"name": "Processor2", "type": "org.apache.nifi.processors.attributes.UpdateAttribute"},
                "status": {"runStatus": "STOPPED"}
            }
        ]
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main,
            [
                "list-processors",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "admin",
                "--password", "admin"
            ]
        )

        assert result.exit_code == 0
        assert "Found 2 processors" in result.output
        assert "NiFi Processors" in result.output
        assert "Processor Type Summary" in result.output

    @patch("nifi2py.cli.NiFiClient")
    def test_list_processors_filter_type(self, mock_client_class, runner):
        """Test processor listing with type filter."""
        mock_client = Mock()
        mock_client.get_root_process_group_id.return_value = "root-id"
        mock_client.list_processors.return_value = [
            {
                "id": "1",
                "component": {"name": "Logger", "type": "org.apache.nifi.processors.standard.LogMessage"},
                "status": {"runStatus": "RUNNING"}
            }
        ]
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main,
            [
                "list-processors",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "admin",
                "--password", "admin",
                "--filter-type", "LogMessage"
            ]
        )

        assert result.exit_code == 0
        assert "Filtered by type: LogMessage" in result.output

    @patch("nifi2py.cli.NiFiClient")
    def test_list_processors_filter_state(self, mock_client_class, runner):
        """Test processor listing with state filter."""
        mock_client = Mock()
        mock_client.get_root_process_group_id.return_value = "root-id"
        mock_client.list_processors.return_value = [
            {
                "id": "1",
                "component": {"name": "Running Proc", "type": "org.apache.nifi.processors.standard.LogMessage"},
                "status": {"runStatus": "RUNNING"}
            }
        ]
        mock_client_class.return_value = mock_client

        result = runner.invoke(
            main,
            [
                "list-processors",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "admin",
                "--password", "admin",
                "--filter-state", "RUNNING"
            ]
        )

        assert result.exit_code == 0
        assert "Filtered by state: RUNNING" in result.output


class TestAnalyze:
    """Tests for the analyze command."""

    @patch("nifi2py.cli.NiFiClient")
    def test_analyze_basic(self, mock_client_class, runner, tmp_path):
        """Test basic flow analysis."""
        mock_client = Mock()
        mock_client.get_root_process_group_id.return_value = "root-id"
        mock_client.list_processors.return_value = [
            {
                "id": "1",
                "component": {"name": "Proc1", "type": "org.apache.nifi.processors.standard.LogMessage"},
                "status": {"runStatus": "RUNNING"}
            },
            {
                "id": "2",
                "component": {"name": "Proc2", "type": "org.apache.nifi.processors.attributes.UpdateAttribute"},
                "status": {"runStatus": "STOPPED"}
            }
        ]
        mock_client_class.return_value = mock_client

        output_file = tmp_path / "report.json"
        result = runner.invoke(
            main,
            [
                "analyze",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "admin",
                "--password", "admin",
                "-o", str(output_file)
            ]
        )

        assert result.exit_code == 0
        assert "Analysis complete" in result.output
        assert "Report saved to" in result.output
        assert output_file.exists()

        # Verify report content
        report = json.loads(output_file.read_text())
        assert "summary" in report
        assert report["summary"]["total_processors"] == 2
        assert "processor_types" in report

    @patch("nifi2py.cli.NiFiClient")
    def test_analyze_missing_output(self, mock_client_class, runner):
        """Test analyze without output file (should fail)."""
        result = runner.invoke(
            main,
            [
                "analyze",
                "--url", "http://localhost:8080/nifi-api",
                "--user", "admin",
                "--password", "admin"
            ]
        )

        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()


class TestGeneratePythonStub:
    """Tests for the Python code generation function."""

    def test_generate_python_stub(self):
        """Test Python stub generation."""
        # Create a simple flow graph
        flow_graph = FlowGraph(
            processors={
                "proc-1": Processor(
                    id="proc-1",
                    name="Test Processor",
                    type="org.apache.nifi.processors.standard.LogMessage",
                    parent_group_id="group-1"
                )
            },
            connections=[],
            template_name="Test Flow"
        )

        code = generate_python_stub(flow_graph)

        assert "class FlowFile:" in code
        assert "class Flow:" in code
        assert "Test Flow" in code
        assert "process_proc_1" in code
        assert "LogMessage" in code

    def test_generate_python_stub_multiple_processors(self):
        """Test stub generation with multiple processors."""
        flow_graph = FlowGraph(
            processors={
                "proc-1": Processor(
                    id="proc-1",
                    name="Processor 1",
                    type="org.apache.nifi.processors.standard.LogMessage",
                    parent_group_id="group-1"
                ),
                "proc-2": Processor(
                    id="proc-2",
                    name="Processor 2",
                    type="org.apache.nifi.processors.attributes.UpdateAttribute",
                    parent_group_id="group-1"
                )
            },
            connections=[],
            template_name="Multi Processor Flow"
        )

        code = generate_python_stub(flow_graph)

        assert "process_proc_1" in code
        assert "process_proc_2" in code
        assert code.count("def process_") == 2


class TestCredentialHandling:
    """Tests for credential handling and environment variables."""

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_credentials_no_env(self, runner):
        """Test that missing credentials are reported."""
        result = runner.invoke(main, ["test-connection"])

        assert result.exit_code != 0
        assert "Error" in result.output

    @patch.dict("os.environ", {"NIFI_URL": "http://localhost:8080/nifi-api"})
    def test_partial_credentials_from_env(self, runner):
        """Test with partial credentials from environment."""
        result = runner.invoke(main, ["test-connection"])

        # Should fail because user and password are missing
        assert result.exit_code != 0
        assert "Error" in result.output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
