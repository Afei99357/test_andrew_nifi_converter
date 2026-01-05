#!/usr/bin/env python3
"""
Provenance-based validation framework

Validates generated Python code against NiFi's actual execution by:
1. Getting input/output content from provenance events
2. Running the same input through generated Python code
3. Comparing outputs byte-for-byte
"""

from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import hashlib


@dataclass
class ValidationResult:
    """Result of validating a single provenance event"""
    event_id: str
    processor_id: str
    processor_name: str
    matches: bool
    nifi_output_hash: str
    python_output_hash: str
    nifi_attributes: Dict[str, str]
    python_attributes: Dict[str, str]
    error: Optional[str] = None


@dataclass
class ValidationSummary:
    """Summary of validation across multiple events"""
    total_events: int
    matched: int
    mismatched: int
    errors: int
    parity_percentage: float
    results: List[ValidationResult]


class ProvenanceValidator:
    """
    Validates generated Python code against NiFi provenance data
    """

    def __init__(self, nifi_client, generated_module):
        """
        Initialize validator

        Args:
            nifi_client: NiFiClient instance
            generated_module: The generated Python module (import it first)
        """
        self.client = nifi_client
        self.module = generated_module

    def _hash_content(self, content: bytes) -> str:
        """Generate hash of content for comparison"""
        return hashlib.sha256(content).hexdigest()[:16]

    def _get_processor_function(self, processor_id: str):
        """Get the generated Python function for a processor"""
        # Function names follow pattern: process_{type}_{id_prefix}
        # Try to find it in the module
        for name in dir(self.module):
            if name.startswith('process_') and processor_id.replace('-', '_')[:16] in name:
                return getattr(self.module, name)
        return None

    def validate_event(
        self,
        event: Dict[str, Any],
        input_content: bytes,
        output_content: bytes,
        shared_state: Optional[Dict] = None
    ) -> ValidationResult:
        """
        Validate a single provenance event

        Args:
            event: Provenance event dict
            input_content: Input FlowFile content from NiFi
            output_content: Output FlowFile content from NiFi
            shared_state: Shared state (e.g., cache for DetectDuplicate)

        Returns:
            ValidationResult with comparison details
        """
        processor_id = event.get('componentId')
        processor_name = event.get('componentName', 'Unknown')
        event_id = str(event.get('eventId'))

        # Get the processor function
        proc_func = self._get_processor_function(processor_id)

        if not proc_func:
            return ValidationResult(
                event_id=event_id,
                processor_id=processor_id,
                processor_name=processor_name,
                matches=False,
                nifi_output_hash="",
                python_output_hash="",
                nifi_attributes={},
                python_attributes={},
                error=f"No function found for processor {processor_id}"
            )

        try:
            # Create FlowFile from input
            from generated.generated_from_provenance import FlowFile
            input_ff = FlowFile(content=input_content, attributes={})

            # Execute Python function
            if shared_state is not None and 'cache' in shared_state:
                # DetectDuplicate needs cache
                result = proc_func(input_ff, shared_state['cache'])
            else:
                result = proc_func(input_ff)

            # Get output FlowFile (first from any relationship)
            python_flowfiles = list(result.values())[0] if result else []

            if not python_flowfiles:
                return ValidationResult(
                    event_id=event_id,
                    processor_id=processor_id,
                    processor_name=processor_name,
                    matches=False,
                    nifi_output_hash=self._hash_content(output_content),
                    python_output_hash="",
                    nifi_attributes={},
                    python_attributes={},
                    error="Python function returned no FlowFiles"
                )

            python_ff = python_flowfiles[0]

            # Compare content
            nifi_hash = self._hash_content(output_content)
            python_hash = self._hash_content(python_ff.content)
            content_matches = nifi_hash == python_hash

            # Compare attributes (from provenance)
            nifi_attributes = event.get('updatedAttributes', {})
            python_attributes = python_ff.attributes

            return ValidationResult(
                event_id=event_id,
                processor_id=processor_id,
                processor_name=processor_name,
                matches=content_matches,
                nifi_output_hash=nifi_hash,
                python_output_hash=python_hash,
                nifi_attributes=nifi_attributes,
                python_attributes=python_attributes
            )

        except Exception as e:
            return ValidationResult(
                event_id=event_id,
                processor_id=processor_id,
                processor_name=processor_name,
                matches=False,
                nifi_output_hash=self._hash_content(output_content),
                python_output_hash="",
                nifi_attributes={},
                python_attributes={},
                error=str(e)
            )

    def validate(
        self,
        processor_id: Optional[str] = None,
        sample_size: int = 10
    ) -> ValidationSummary:
        """
        Validate generated code against provenance events

        Args:
            processor_id: Optional processor ID to validate (or None for all)
            sample_size: Number of events to validate per processor

        Returns:
            ValidationSummary with results
        """
        # Query provenance
        events = self.client.query_provenance(
            processor_id=processor_id,
            max_events=sample_size
        )

        if not events:
            return ValidationSummary(
                total_events=0,
                matched=0,
                mismatched=0,
                errors=0,
                parity_percentage=0.0,
                results=[]
            )

        results = []
        shared_state = {'cache': set()}  # For DetectDuplicate

        for event in events[:sample_size]:
            event_id = event.get('eventId')

            # Get input/output content from provenance
            try:
                # Note: This requires the provenance event to have content
                # In practice, content might not be available for all events
                # This is a limitation of the NiFi API - content is only retained
                # for a configurable period
                input_content = self.client.get_provenance_event_content(
                    event_id, 'input'
                )
                output_content = self.client.get_provenance_event_content(
                    event_id, 'output'
                )

                # Validate this event
                result = self.validate_event(
                    event, input_content, output_content, shared_state
                )
                results.append(result)

            except Exception as e:
                # Content not available or other error
                results.append(ValidationResult(
                    event_id=str(event_id),
                    processor_id=event.get('componentId', ''),
                    processor_name=event.get('componentName', 'Unknown'),
                    matches=False,
                    nifi_output_hash="",
                    python_output_hash="",
                    nifi_attributes={},
                    python_attributes={},
                    error=f"Failed to get content: {e}"
                ))

        # Calculate summary
        matched = sum(1 for r in results if r.matches)
        errors = sum(1 for r in results if r.error)
        mismatched = len(results) - matched - errors

        parity = (matched / len(results) * 100) if results else 0.0

        return ValidationSummary(
            total_events=len(results),
            matched=matched,
            mismatched=mismatched,
            errors=errors,
            parity_percentage=parity,
            results=results
        )


# Note: The NiFi client needs a method to get provenance event content
# This should be added to client.py:
#
# def get_provenance_event_content(self, event_id: str, direction: str) -> bytes:
#     """
#     Get FlowFile content from a provenance event
#
#     Args:
#         event_id: Provenance event ID
#         direction: 'input' or 'output'
#
#     Returns:
#         FlowFile content as bytes
#     """
#     endpoint = f"/provenance-events/{event_id}/content/{direction}"
#     response = self._request("GET", endpoint)
#     return response.content
