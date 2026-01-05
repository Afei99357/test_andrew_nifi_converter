"""
Extract provenance data from running NiFi flow for Python code generation.

This module provides tools to extract real execution samples from NiFi provenance,
including input/output content and attribute transformations. This data is used
to inform code generation and validation.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import hashlib
import logging

from nifi2py.client import NiFiClient
from nifi2py.models import ProvenanceEvent, FlowFile

logger = logging.getLogger(__name__)


@dataclass
class ExecutionSample:
    """Single execution sample from provenance"""

    event_id: int
    timestamp: datetime

    # Input
    input_content: Optional[bytes]
    input_attributes: Dict[str, str]

    # Output
    output_content: Optional[bytes]
    output_attributes: Dict[str, str]

    # Transformation
    attributes_added: Dict[str, str]
    attributes_modified: Dict[str, str]
    attributes_removed: List[str]
    content_changed: bool

    def __repr__(self) -> str:
        return (
            f"ExecutionSample(event_id={self.event_id}, "
            f"content_changed={self.content_changed}, "
            f"attrs_added={len(self.attributes_added)}, "
            f"attrs_modified={len(self.attributes_modified)}, "
            f"attrs_removed={len(self.attributes_removed)})"
        )


@dataclass
class ProcessorExecution:
    """Captured execution of a processor from provenance"""

    processor_id: str
    processor_name: str
    processor_type: str

    # Execution samples
    executions: List[ExecutionSample] = field(default_factory=list)

    # Statistics
    total_executions: int = 0
    success_count: int = 0
    failure_count: int = 0

    def __repr__(self) -> str:
        return (
            f"ProcessorExecution(name='{self.processor_name}', "
            f"type='{self.processor_type.split('.')[-1]}', "
            f"samples={len(self.executions)})"
        )

    @property
    def has_samples(self) -> bool:
        """Check if we have any execution samples."""
        return len(self.executions) > 0

    @property
    def sample_coverage(self) -> float:
        """Get percentage of executions we have samples for."""
        if self.total_executions == 0:
            return 0.0
        return (self.success_count / self.total_executions) * 100.0


class ProvenanceExtractor:
    """Extract processor execution patterns from NiFi provenance"""

    def __init__(self, client: NiFiClient):
        """
        Initialize provenance extractor.

        Args:
            client: Authenticated NiFi client
        """
        self.client = client

    def extract_processor_executions(
        self,
        processor_id: str,
        sample_size: int = 10,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> Optional[ProcessorExecution]:
        """
        Extract execution samples for a processor.

        Args:
            processor_id: Processor to analyze
            sample_size: Number of samples to collect
            start_time: Start of time window (default: last hour)
            end_time: End of time window (default: now)

        Returns:
            ProcessorExecution with samples, or None if extraction failed
        """
        if start_time is None:
            start_time = datetime.now() - timedelta(hours=1)
        if end_time is None:
            end_time = datetime.now()

        logger.info(f"Extracting provenance for processor {processor_id}")

        # Get processor info first
        try:
            proc_info = self.client.get_processor(processor_id)
        except Exception as e:
            logger.error(f"Failed to get processor info: {e}")
            return None

        processor_name = proc_info["component"]["name"]
        processor_type = proc_info["component"]["type"]

        # Query provenance
        try:
            events = self.client.query_provenance(
                processor_id=processor_id,
                start_date=start_time,
                end_date=end_time,
                max_results=sample_size * 2,  # Request more to account for failures
            )
            logger.info(f"Found {len(events)} provenance events")
        except Exception as e:
            # Handle 403 or other errors gracefully
            logger.warning(f"Could not query provenance: {e}")
            # Return object with no samples
            return ProcessorExecution(
                processor_id=processor_id,
                processor_name=processor_name,
                processor_type=processor_type,
                executions=[],
                total_executions=0,
                success_count=0,
                failure_count=0,
            )

        # Extract samples
        samples = []
        for event in events[:sample_size]:
            sample = self._extract_execution_sample(event)
            if sample:
                samples.append(sample)

        success_count = len(samples)
        failure_count = len(events) - success_count

        return ProcessorExecution(
            processor_id=processor_id,
            processor_name=processor_name,
            processor_type=processor_type,
            executions=samples,
            total_executions=len(events),
            success_count=success_count,
            failure_count=failure_count,
        )

    def _extract_execution_sample(self, event: Dict) -> Optional[ExecutionSample]:
        """
        Extract single execution sample from provenance event.

        Args:
            event: Provenance event data from API

        Returns:
            ExecutionSample or None if extraction failed
        """
        try:
            event_id = int(event["eventId"])
            timestamp_str = event.get("eventTime", event.get("timestamp"))

            # Parse timestamp
            if timestamp_str:
                # NiFi timestamp format: "MM/dd/yyyy HH:mm:ss.SSS zzz"
                try:
                    timestamp = datetime.strptime(
                        timestamp_str.split(".")[0], "%m/%d/%Y %H:%M:%S"
                    )
                except:
                    timestamp = datetime.now()
            else:
                timestamp = datetime.now()

            # Get input content (may not be available)
            input_content = None
            try:
                input_content = self.client.get_provenance_content(event_id, "input")
                logger.debug(f"Retrieved input content for event {event_id}")
            except Exception as e:
                logger.debug(f"No input content for event {event_id}: {e}")

            # Get output content (may not be available)
            output_content = None
            try:
                output_content = self.client.get_provenance_content(event_id, "output")
                logger.debug(f"Retrieved output content for event {event_id}")
            except Exception as e:
                logger.debug(f"No output content for event {event_id}: {e}")

            # Extract attributes
            input_attrs = event.get("inputAttributes", {})
            output_attrs = event.get("outputAttributes", {})

            # Calculate attribute diff
            added = {k: v for k, v in output_attrs.items() if k not in input_attrs}
            modified = {
                k: output_attrs[k]
                for k in output_attrs
                if k in input_attrs and input_attrs[k] != output_attrs[k]
            }
            removed = [k for k in input_attrs if k not in output_attrs]

            # Check if content changed
            content_changed = False
            if input_content is not None and output_content is not None:
                content_changed = input_content != output_content
            elif input_content != output_content:  # One is None, other isn't
                content_changed = True

            return ExecutionSample(
                event_id=event_id,
                timestamp=timestamp,
                input_content=input_content,
                input_attributes=input_attrs,
                output_content=output_content,
                output_attributes=output_attrs,
                attributes_added=added,
                attributes_modified=modified,
                attributes_removed=removed,
                content_changed=content_changed,
            )
        except Exception as e:
            logger.warning(f"Failed to extract sample from event: {e}")
            return None

    def extract_all_executions(
        self, processor_ids: List[str], sample_size: int = 10
    ) -> Dict[str, ProcessorExecution]:
        """
        Extract executions for multiple processors.

        Args:
            processor_ids: List of processor IDs to analyze
            sample_size: Number of samples per processor

        Returns:
            Dict mapping processor_id to ProcessorExecution
        """
        results = {}

        for proc_id in processor_ids:
            logger.info(f"Extracting provenance for {proc_id}...")
            execution = self.extract_processor_executions(proc_id, sample_size)
            if execution:
                results[proc_id] = execution

        return results

    def extract_flow_executions(
        self, group_id: Optional[str] = None, sample_size: int = 10
    ) -> Dict[str, ProcessorExecution]:
        """
        Extract executions for all processors in a process group.

        Args:
            group_id: Process group ID (None for root)
            sample_size: Number of samples per processor

        Returns:
            Dict mapping processor_id to ProcessorExecution
        """
        # Get all processors in group
        processors = self.client.list_processors(group_id)
        processor_ids = [p["id"] for p in processors]

        logger.info(f"Extracting provenance for {len(processor_ids)} processors")

        return self.extract_all_executions(processor_ids, sample_size)

    def get_attribute_patterns(
        self, executions: List[ExecutionSample]
    ) -> Dict[str, Dict[str, int]]:
        """
        Analyze attribute transformation patterns across multiple executions.

        Args:
            executions: List of execution samples

        Returns:
            Dict mapping attribute names to their transformation patterns:
            {
                "filename": {"added": 5, "modified": 2, "removed": 0},
                "timestamp": {"added": 10, "modified": 0, "removed": 0},
                ...
            }
        """
        patterns: Dict[str, Dict[str, int]] = {}

        for sample in executions:
            # Track added attributes
            for attr in sample.attributes_added:
                if attr not in patterns:
                    patterns[attr] = {"added": 0, "modified": 0, "removed": 0}
                patterns[attr]["added"] += 1

            # Track modified attributes
            for attr in sample.attributes_modified:
                if attr not in patterns:
                    patterns[attr] = {"added": 0, "modified": 0, "removed": 0}
                patterns[attr]["modified"] += 1

            # Track removed attributes
            for attr in sample.attributes_removed:
                if attr not in patterns:
                    patterns[attr] = {"added": 0, "modified": 0, "removed": 0}
                patterns[attr]["removed"] += 1

        return patterns

    def get_content_transformation_summary(
        self, executions: List[ExecutionSample]
    ) -> Dict[str, int]:
        """
        Summarize content transformation patterns.

        Args:
            executions: List of execution samples

        Returns:
            Dict with transformation statistics
        """
        total = len(executions)
        content_changed = sum(1 for s in executions if s.content_changed)
        content_unchanged = total - content_changed

        return {
            "total_samples": total,
            "content_changed": content_changed,
            "content_unchanged": content_unchanged,
            "change_percentage": (content_changed / total * 100) if total > 0 else 0,
        }
