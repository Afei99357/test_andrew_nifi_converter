"""
Unit tests for provenance extractor.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
from nifi2py.provenance_extractor import (
    ProvenanceExtractor,
    ProcessorExecution,
    ExecutionSample,
)


class TestExecutionSample:
    """Test ExecutionSample dataclass."""

    def test_execution_sample_creation(self):
        sample = ExecutionSample(
            event_id=12345,
            timestamp=datetime.now(),
            input_content=b"test input",
            input_attributes={"filename": "test.txt"},
            output_content=b"test output",
            output_attributes={"filename": "test.txt", "processed": "true"},
            attributes_added={"processed": "true"},
            attributes_modified={},
            attributes_removed=[],
            content_changed=True,
        )

        assert sample.event_id == 12345
        assert sample.content_changed is True
        assert len(sample.attributes_added) == 1
        assert sample.attributes_added["processed"] == "true"

    def test_execution_sample_repr(self):
        sample = ExecutionSample(
            event_id=12345,
            timestamp=datetime.now(),
            input_content=None,
            input_attributes={},
            output_content=None,
            output_attributes={},
            attributes_added={},
            attributes_modified={},
            attributes_removed=[],
            content_changed=False,
        )

        repr_str = repr(sample)
        assert "ExecutionSample" in repr_str
        assert "12345" in repr_str


class TestProcessorExecution:
    """Test ProcessorExecution dataclass."""

    def test_processor_execution_creation(self):
        execution = ProcessorExecution(
            processor_id="proc-1",
            processor_name="Test Processor",
            processor_type="org.apache.nifi.processors.standard.UpdateAttribute",
            executions=[],
            total_executions=10,
            success_count=8,
            failure_count=2,
        )

        assert execution.processor_id == "proc-1"
        assert execution.total_executions == 10
        assert execution.success_count == 8
        assert execution.failure_count == 2

    def test_has_samples(self):
        execution = ProcessorExecution(
            processor_id="proc-1",
            processor_name="Test",
            processor_type="UpdateAttribute",
        )
        assert execution.has_samples is False

        execution.executions.append(
            ExecutionSample(
                event_id=1,
                timestamp=datetime.now(),
                input_content=None,
                input_attributes={},
                output_content=None,
                output_attributes={},
                attributes_added={},
                attributes_modified={},
                attributes_removed=[],
                content_changed=False,
            )
        )
        assert execution.has_samples is True

    def test_sample_coverage(self):
        execution = ProcessorExecution(
            processor_id="proc-1",
            processor_name="Test",
            processor_type="UpdateAttribute",
            total_executions=10,
            success_count=7,
        )
        assert execution.sample_coverage == 70.0

        # Test zero division
        execution.total_executions = 0
        assert execution.sample_coverage == 0.0


class TestProvenanceExtractor:
    """Test ProvenanceExtractor class."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock NiFi client."""
        client = Mock()
        client.get_processor.return_value = {
            "component": {
                "name": "Test Processor",
                "type": "org.apache.nifi.processors.standard.UpdateAttribute",
            }
        }
        return client

    @pytest.fixture
    def extractor(self, mock_client):
        """Create a provenance extractor with mock client."""
        return ProvenanceExtractor(mock_client)

    def test_extractor_creation(self, mock_client):
        """Test creating an extractor."""
        extractor = ProvenanceExtractor(mock_client)
        assert extractor.client == mock_client

    def test_extract_processor_executions_no_events(self, extractor, mock_client):
        """Test extraction when no provenance events are found."""
        mock_client.query_provenance.return_value = []

        result = extractor.extract_processor_executions("proc-1", sample_size=5)

        assert result is not None
        assert result.processor_id == "proc-1"
        assert result.processor_name == "Test Processor"
        assert len(result.executions) == 0
        assert result.total_executions == 0

    def test_extract_processor_executions_with_events(self, extractor, mock_client):
        """Test extraction with provenance events."""
        # Mock provenance events
        mock_events = [
            {
                "eventId": "12345",
                "eventTime": "01/03/2026 10:00:00.000 PST",
                "inputAttributes": {"filename": "test.txt"},
                "outputAttributes": {"filename": "test.txt", "timestamp": "2026-01-03"},
            },
            {
                "eventId": "12346",
                "eventTime": "01/03/2026 10:00:01.000 PST",
                "inputAttributes": {"filename": "test2.txt"},
                "outputAttributes": {"filename": "test2.txt", "timestamp": "2026-01-03"},
            },
        ]

        mock_client.query_provenance.return_value = mock_events
        mock_client.get_provenance_content.side_effect = Exception("No content available")

        result = extractor.extract_processor_executions("proc-1", sample_size=5)

        assert result is not None
        assert result.processor_id == "proc-1"
        assert len(result.executions) == 2
        assert result.total_executions == 2
        assert result.success_count == 2

        # Check first sample
        sample = result.executions[0]
        assert sample.event_id == 12345
        assert "filename" in sample.input_attributes
        assert "timestamp" in sample.attributes_added

    def test_extract_processor_executions_with_content(self, extractor, mock_client):
        """Test extraction when content is available."""
        mock_events = [
            {
                "eventId": "12345",
                "eventTime": "01/03/2026 10:00:00.000 PST",
                "inputAttributes": {},
                "outputAttributes": {},
            }
        ]

        mock_client.query_provenance.return_value = mock_events

        # Mock content retrieval
        def get_content(event_id, direction):
            if direction == "input":
                return b"input content"
            else:
                return b"output content"

        mock_client.get_provenance_content.side_effect = get_content

        result = extractor.extract_processor_executions("proc-1", sample_size=5)

        assert result is not None
        assert len(result.executions) == 1

        sample = result.executions[0]
        assert sample.input_content == b"input content"
        assert sample.output_content == b"output content"
        assert sample.content_changed is True

    def test_extract_processor_executions_provenance_error(self, extractor, mock_client):
        """Test graceful handling of provenance query errors."""
        mock_client.query_provenance.side_effect = Exception("403 Forbidden")

        result = extractor.extract_processor_executions("proc-1", sample_size=5)

        # Should return empty result, not raise exception
        assert result is not None
        assert len(result.executions) == 0
        assert result.total_executions == 0

    def test_get_attribute_patterns(self, extractor):
        """Test attribute pattern analysis."""
        samples = [
            ExecutionSample(
                event_id=1,
                timestamp=datetime.now(),
                input_content=None,
                input_attributes={},
                output_content=None,
                output_attributes={},
                attributes_added={"timestamp": "2026-01-03"},
                attributes_modified={"filename": "new.txt"},
                attributes_removed=["old_attr"],
                content_changed=False,
            ),
            ExecutionSample(
                event_id=2,
                timestamp=datetime.now(),
                input_content=None,
                input_attributes={},
                output_content=None,
                output_attributes={},
                attributes_added={"timestamp": "2026-01-03"},
                attributes_modified={},
                attributes_removed=[],
                content_changed=False,
            ),
        ]

        patterns = extractor.get_attribute_patterns(samples)

        assert "timestamp" in patterns
        assert patterns["timestamp"]["added"] == 2
        assert patterns["timestamp"]["modified"] == 0

        assert "filename" in patterns
        assert patterns["filename"]["modified"] == 1

        assert "old_attr" in patterns
        assert patterns["old_attr"]["removed"] == 1

    def test_get_content_transformation_summary(self, extractor):
        """Test content transformation summary."""
        samples = [
            ExecutionSample(
                event_id=1,
                timestamp=datetime.now(),
                input_content=None,
                input_attributes={},
                output_content=None,
                output_attributes={},
                attributes_added={},
                attributes_modified={},
                attributes_removed=[],
                content_changed=True,
            ),
            ExecutionSample(
                event_id=2,
                timestamp=datetime.now(),
                input_content=None,
                input_attributes={},
                output_content=None,
                output_attributes={},
                attributes_added={},
                attributes_modified={},
                attributes_removed=[],
                content_changed=False,
            ),
            ExecutionSample(
                event_id=3,
                timestamp=datetime.now(),
                input_content=None,
                input_attributes={},
                output_content=None,
                output_attributes={},
                attributes_added={},
                attributes_modified={},
                attributes_removed=[],
                content_changed=True,
            ),
        ]

        summary = extractor.get_content_transformation_summary(samples)

        assert summary["total_samples"] == 3
        assert summary["content_changed"] == 2
        assert summary["content_unchanged"] == 1
        assert summary["change_percentage"] == pytest.approx(66.67, rel=0.1)

    def test_extract_all_executions(self, extractor, mock_client):
        """Test extracting executions for multiple processors."""
        mock_client.query_provenance.return_value = []

        processor_ids = ["proc-1", "proc-2", "proc-3"]
        results = extractor.extract_all_executions(processor_ids, sample_size=5)

        assert len(results) == 3
        assert "proc-1" in results
        assert "proc-2" in results
        assert "proc-3" in results

    def test_extract_sample_with_malformed_event(self, extractor):
        """Test handling of malformed provenance events."""
        # Event with missing required fields
        malformed_event = {"some_field": "value"}

        sample = extractor._extract_execution_sample(malformed_event)

        # Should return None for malformed events
        assert sample is None

    def test_extract_sample_attribute_diff_calculation(self, extractor, mock_client):
        """Test correct calculation of attribute differences."""
        event = {
            "eventId": "12345",
            "eventTime": "01/03/2026 10:00:00.000 PST",
            "inputAttributes": {
                "filename": "test.txt",
                "size": "100",
                "old_attr": "value",
            },
            "outputAttributes": {
                "filename": "test.txt",
                "size": "200",  # Modified
                "new_attr": "new",  # Added
                # old_attr removed
            },
        }

        mock_client.get_provenance_content.side_effect = Exception("No content")

        sample = extractor._extract_execution_sample(event)

        assert sample is not None
        assert "new_attr" in sample.attributes_added
        assert "size" in sample.attributes_modified
        assert "old_attr" in sample.attributes_removed
        assert "filename" not in sample.attributes_added
        assert "filename" not in sample.attributes_modified
