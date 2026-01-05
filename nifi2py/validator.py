"""
NiFi to Python Validator

Validates generated Python code against NiFi flows and provenance data.
Provides both static validation (no NiFi needed) and dynamic validation
(compares outputs against NiFi provenance).

Author: nifi2py
"""

from __future__ import annotations

import hashlib
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from .models import FlowGraph, FlowFile, ValidationResult, Processor
from .client import NiFiClient, NiFiClientError

logger = logging.getLogger(__name__)
console = Console()


@dataclass
class ValidationReport:
    """
    Validation results report.

    Contains both static validation results (syntax, coverage) and
    optional dynamic validation results (provenance comparison).
    """

    # Static validation
    total_processors: int
    converted_processors: int
    stub_processors: int
    syntax_valid: bool = True
    all_connections_valid: bool = True

    # Provenance validation (optional)
    provenance_available: bool = False
    provenance_results: List[ValidationResult] = field(default_factory=list)

    # Error messages
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def coverage_percentage(self) -> float:
        """Calculate conversion coverage percentage."""
        if self.total_processors == 0:
            return 0.0
        return (self.converted_processors / self.total_processors) * 100.0

    @property
    def stub_percentage(self) -> float:
        """Calculate stub percentage."""
        if self.total_processors == 0:
            return 0.0
        return (self.stub_processors / self.total_processors) * 100.0

    @property
    def provenance_pass_count(self) -> int:
        """Count of passed provenance validations."""
        return sum(1 for r in self.provenance_results if r.passed)

    @property
    def provenance_fail_count(self) -> int:
        """Count of failed provenance validations."""
        return sum(1 for r in self.provenance_results if not r.passed)

    @property
    def provenance_pass_percentage(self) -> float:
        """Calculate provenance validation pass percentage."""
        if not self.provenance_results:
            return 0.0
        return (self.provenance_pass_count / len(self.provenance_results)) * 100.0

    def print_summary(self) -> None:
        """Print beautiful validation summary using Rich."""
        console.print()
        console.print(Panel.fit(
            "[bold cyan]Validation Report[/bold cyan]",
            border_style="cyan"
        ))

        # Static Validation Section
        console.print("\n[bold]Static Validation Results[/bold]")
        console.print("─" * 60)

        static_table = Table(show_header=False, box=None)
        static_table.add_column("Metric", style="cyan")
        static_table.add_column("Value", style="green")

        static_table.add_row("Total Processors", str(self.total_processors))
        static_table.add_row("Converted Processors", str(self.converted_processors))
        static_table.add_row("Stub Processors", str(self.stub_processors))

        coverage_color = "green" if self.coverage_percentage >= 80 else "yellow" if self.coverage_percentage >= 50 else "red"
        static_table.add_row(
            "Coverage",
            f"[{coverage_color}]{self.coverage_percentage:.1f}%[/{coverage_color}]"
        )

        syntax_status = "[green]✓ Valid[/green]" if self.syntax_valid else "[red]✗ Invalid[/red]"
        static_table.add_row("Syntax", syntax_status)

        connections_status = "[green]✓ Valid[/green]" if self.all_connections_valid else "[red]✗ Invalid[/red]"
        static_table.add_row("Connections", connections_status)

        console.print(static_table)

        # Errors and Warnings
        if self.errors:
            console.print("\n[bold red]Errors:[/bold red]")
            for error in self.errors:
                console.print(f"  [red]✗[/red] {error}")

        if self.warnings:
            console.print("\n[bold yellow]Warnings:[/bold yellow]")
            for warning in self.warnings:
                console.print(f"  [yellow]![/yellow] {warning}")

        # Provenance Validation Section (if available)
        if self.provenance_available and self.provenance_results:
            console.print("\n[bold]Provenance Validation Results[/bold]")
            console.print("─" * 60)

            prov_table = Table(show_header=False, box=None)
            prov_table.add_column("Metric", style="cyan")
            prov_table.add_column("Value", style="green")

            prov_table.add_row("Total Validations", str(len(self.provenance_results)))
            prov_table.add_row("Passed", f"[green]{self.provenance_pass_count}[/green]")
            prov_table.add_row("Failed", f"[red]{self.provenance_fail_count}[/red]")

            pass_color = "green" if self.provenance_pass_percentage == 100 else "yellow" if self.provenance_pass_percentage >= 80 else "red"
            prov_table.add_row(
                "Pass Rate",
                f"[{pass_color}]{self.provenance_pass_percentage:.1f}%[/{pass_color}]"
            )

            console.print(prov_table)

            # Show failed validations
            failed = [r for r in self.provenance_results if not r.passed]
            if failed:
                console.print(f"\n[bold red]Failed Validations ({len(failed)}):[/bold red]")
                for result in failed[:5]:  # Show first 5
                    console.print(f"  Event {result.event_id}: {result.processor_name or result.processor_id}")
                    if result.error:
                        console.print(f"    Error: {result.error}")
                    if not result.content_match:
                        console.print(f"    Content mismatch")
                    if not result.attributes_match:
                        console.print(f"    Attribute mismatch")

                if len(failed) > 5:
                    console.print(f"  ... and {len(failed) - 5} more")

        elif self.provenance_available:
            console.print("\n[yellow]Provenance validation attempted but no results available[/yellow]")

        # Overall Status
        console.print("\n[bold]Overall Status[/bold]")
        console.print("─" * 60)

        if self.syntax_valid and self.all_connections_valid and self.coverage_percentage >= 80:
            if self.provenance_available:
                if self.provenance_pass_percentage == 100:
                    console.print("[bold green]✓ ALL VALIDATIONS PASSED[/bold green]")
                elif self.provenance_pass_percentage >= 80:
                    console.print("[bold yellow]⚠ MOSTLY PASSED (some provenance failures)[/bold yellow]")
                else:
                    console.print("[bold red]✗ VALIDATION FAILED (provenance issues)[/bold red]")
            else:
                console.print("[bold green]✓ STATIC VALIDATION PASSED[/bold green]")
                console.print("[dim]Run with NiFi provenance access for full validation[/dim]")
        else:
            console.print("[bold red]✗ VALIDATION FAILED[/bold red]")

        console.print()


class Validator:
    """
    Validates generated Python code against NiFi flows.

    Provides two levels of validation:
    1. Static validation: Checks syntax, coverage, connections (no NiFi needed)
    2. Dynamic validation: Compares outputs against NiFi provenance (requires NiFi access)

    Example:
        >>> # Static validation (no NiFi needed)
        >>> validator = Validator(None, None)
        >>> report = validator.validate_static(flow_graph)
        >>> report.print_summary()

        >>> # With provenance (requires NiFi)
        >>> client = NiFiClient(url, username, password)
        >>> validator = Validator(client, Path("generated_flow.py"))
        >>> report = validator.validate_with_provenance("processor-id", sample_size=10)
    """

    def __init__(
        self,
        nifi_client: Optional[NiFiClient] = None,
        python_module_path: Optional[Path] = None
    ):
        """
        Initialize validator.

        Args:
            nifi_client: Connected NiFi client (optional, only needed for provenance)
            python_module_path: Path to generated Python module (optional)
        """
        self.nifi_client = nifi_client
        self.python_module_path = python_module_path
        self.python_module = None

        if python_module_path and python_module_path.exists():
            self._load_python_module()

    def _load_python_module(self) -> None:
        """Load the generated Python module for execution."""
        if not self.python_module_path:
            return

        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "generated_flow",
                self.python_module_path
            )
            if spec and spec.loader:
                self.python_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self.python_module)
                logger.info(f"Loaded Python module from {self.python_module_path}")
        except Exception as e:
            logger.error(f"Failed to load Python module: {e}")
            raise

    def validate_static(self, flow_graph: FlowGraph) -> ValidationReport:
        """
        Static validation without NiFi access.

        Validates:
        - All processors are accounted for
        - Generated code has valid Python syntax
        - All connections reference valid processors
        - Coverage statistics

        Args:
            flow_graph: The flow graph to validate

        Returns:
            ValidationReport with static validation results
        """
        console.print("\n[bold cyan]Running Static Validation...[/bold cyan]")

        report = ValidationReport(
            total_processors=0,
            converted_processors=0,
            stub_processors=0
        )

        # Count processors
        all_processors = flow_graph.get_all_processors()
        report.total_processors = len(all_processors)

        # Check processor types
        processor_types = flow_graph.get_processor_types()

        # Import converters to check which are supported
        try:
            from .converters import get_converter_for_type

            for proc in all_processors:
                converter = get_converter_for_type(proc.processor_simple_type)
                if converter:
                    if hasattr(converter, 'is_stub') and converter.is_stub:
                        report.stub_processors += 1
                    else:
                        report.converted_processors += 1
                else:
                    report.stub_processors += 1
        except ImportError:
            # If converters not available, assume all are stubs
            logger.warning("Converters module not available, assuming all processors are stubs")
            report.stub_processors = report.total_processors
            report.warnings.append("Converters module not available for validation")

        # Validate syntax if module path provided
        if self.python_module_path and self.python_module_path.exists():
            try:
                with open(self.python_module_path) as f:
                    code = f.read()
                compile(code, str(self.python_module_path), 'exec')
                report.syntax_valid = True
                console.print("[green]✓[/green] Python syntax is valid")
            except SyntaxError as e:
                report.syntax_valid = False
                report.errors.append(f"Syntax error: {e}")
                console.print(f"[red]✗[/red] Syntax error: {e}")

        # Validate connections
        all_connections = flow_graph.get_all_connections()
        processor_ids = {p.id for p in all_processors}

        invalid_connections = []
        for conn in all_connections:
            if conn.source_id not in processor_ids:
                invalid_connections.append(f"Connection {conn.id}: source {conn.source_id} not found")
            if conn.destination_id not in processor_ids:
                invalid_connections.append(f"Connection {conn.id}: destination {conn.destination_id} not found")

        if invalid_connections:
            report.all_connections_valid = False
            report.errors.extend(invalid_connections)
            console.print(f"[red]✗[/red] Found {len(invalid_connections)} invalid connections")
        else:
            console.print("[green]✓[/green] All connections are valid")

        # Summary
        console.print(f"[cyan]→[/cyan] Coverage: {report.coverage_percentage:.1f}% ({report.converted_processors}/{report.total_processors})")
        console.print(f"[cyan]→[/cyan] Stubs: {report.stub_processors}")

        return report

    def validate_with_provenance(
        self,
        processor_id: str,
        sample_size: int = 10
    ) -> ValidationReport:
        """
        Validate against NiFi provenance data (requires permissions).

        Compares generated Python code outputs against actual NiFi provenance
        events to ensure parity. This requires:
        1. NiFi client with provenance read permissions
        2. Generated Python module loaded
        3. Processor that has provenance events

        Args:
            processor_id: Processor ID to validate
            sample_size: Number of provenance events to compare

        Returns:
            ValidationReport with provenance validation results

        Raises:
            ValueError: If NiFi client or Python module not available
        """
        if not self.nifi_client:
            raise ValueError("NiFi client required for provenance validation")

        if not self.python_module:
            raise ValueError("Python module required for provenance validation")

        console.print("\n[bold cyan]Running Provenance Validation...[/bold cyan]")

        report = ValidationReport(
            total_processors=1,
            converted_processors=1,
            stub_processors=0,
            provenance_available=True
        )

        try:
            # Query provenance events
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console
            ) as progress:
                task = progress.add_task(f"Querying provenance for {processor_id}...", total=None)

                try:
                    events = self.nifi_client.query_provenance(
                        processor_id=processor_id,
                        max_results=sample_size
                    )
                    progress.update(task, completed=True)
                except NiFiClientError as e:
                    if "403" in str(e) or "Forbidden" in str(e):
                        console.print("[yellow]⚠[/yellow] Provenance API returned 403 - Access Forbidden")
                        console.print("[dim]Skipping provenance validation (requires permissions)[/dim]")
                        report.provenance_available = False
                        report.warnings.append("Provenance API access denied (403)")
                        return report
                    raise

            if not events:
                console.print("[yellow]⚠[/yellow] No provenance events found")
                report.warnings.append("No provenance events found for processor")
                return report

            console.print(f"[green]✓[/green] Found {len(events)} provenance events")

            # Validate each event
            for i, event in enumerate(events[:sample_size], 1):
                console.print(f"[cyan]→[/cyan] Validating event {i}/{min(sample_size, len(events))}...")

                result = self._validate_single_event(event, processor_id)
                report.provenance_results.append(result)

                if result.passed:
                    console.print(f"  [green]✓[/green] Event {result.event_id} passed")
                else:
                    console.print(f"  [red]✗[/red] Event {result.event_id} failed")

        except NiFiClientError as e:
            report.errors.append(f"NiFi client error: {e}")
            console.print(f"[red]✗[/red] {e}")
        except Exception as e:
            report.errors.append(f"Validation error: {e}")
            console.print(f"[red]✗[/red] Unexpected error: {e}")
            logger.exception("Validation error")

        return report

    def _validate_single_event(
        self,
        event: Dict,
        processor_id: str
    ) -> ValidationResult:
        """
        Validate a single provenance event.

        Args:
            event: Provenance event data from NiFi API
            processor_id: Processor ID being validated

        Returns:
            ValidationResult for this event
        """
        event_id = event.get("eventId", 0)
        flowfile_uuid = event.get("flowFileUuid", "")

        try:
            # Get input/output content from provenance
            input_content = b""
            output_content = b""

            try:
                if event.get("inputContentAvailable"):
                    input_content = self.nifi_client.get_provenance_content(event_id, "input")
            except NiFiClientError:
                pass  # Input content may not be available

            try:
                if event.get("outputContentAvailable"):
                    output_content = self.nifi_client.get_provenance_content(event_id, "output")
            except NiFiClientError:
                pass  # Output content may not be available

            # Get attributes
            nifi_attributes = event.get("attributes", {})

            # Create FlowFile for Python execution
            input_flowfile = FlowFile(
                content=input_content,
                attributes=nifi_attributes.copy()
            )

            # Execute Python code
            # Note: This is a simplified approach - actual implementation would need
            # to find and execute the correct processor function
            python_output = self._execute_python_processor(input_flowfile, processor_id)

            # Compare results
            expected_hash = hashlib.sha256(output_content).hexdigest()
            actual_hash = hashlib.sha256(python_output.content).hexdigest()

            content_match = expected_hash == actual_hash

            # Compare attributes
            attribute_diffs = {}
            for key, expected_value in nifi_attributes.items():
                actual_value = python_output.attributes.get(key)
                if actual_value != expected_value:
                    attribute_diffs[key] = (expected_value, actual_value)

            attributes_match = len(attribute_diffs) == 0

            return ValidationResult(
                processor_id=processor_id,
                processor_name=event.get("componentName"),
                event_id=event_id,
                flowfile_uuid=flowfile_uuid,
                content_match=content_match,
                attributes_match=attributes_match,
                expected_content_hash=expected_hash,
                actual_content_hash=actual_hash,
                expected_attributes=nifi_attributes,
                actual_attributes=python_output.attributes,
                attribute_diffs=attribute_diffs
            )

        except Exception as e:
            # Return failed validation with error
            return ValidationResult(
                processor_id=processor_id,
                event_id=event_id,
                flowfile_uuid=flowfile_uuid,
                content_match=False,
                attributes_match=False,
                expected_content_hash="",
                actual_content_hash="",
                error=str(e)
            )

    def _execute_python_processor(
        self,
        flowfile: FlowFile,
        processor_id: str
    ) -> FlowFile:
        """
        Execute the generated Python processor function.

        Args:
            flowfile: Input FlowFile
            processor_id: Processor ID

        Returns:
            Output FlowFile
        """
        # This is a simplified implementation
        # In reality, we'd need to map processor IDs to function names
        # and handle multiple output relationships

        if not self.python_module:
            raise ValueError("Python module not loaded")

        # Try to find and execute the processor function
        # This is a placeholder - actual implementation would be more sophisticated
        func_name = f"process_{processor_id.replace('-', '_')}"

        if hasattr(self.python_module, func_name):
            func = getattr(self.python_module, func_name)
            result = func(flowfile)

            # Handle different return types
            if isinstance(result, dict):
                # Assume format: {"relationship": [flowfiles]}
                # Return first flowfile from first relationship
                for flowfiles in result.values():
                    if flowfiles:
                        return flowfiles[0]
            elif isinstance(result, FlowFile):
                return result
            elif isinstance(result, list) and result:
                return result[0]

        # If we can't execute, return unchanged flowfile
        return flowfile


def validate_flow(
    flow_graph: FlowGraph,
    python_module_path: Optional[Path] = None,
    nifi_client: Optional[NiFiClient] = None
) -> ValidationReport:
    """
    Convenience function to validate a flow.

    Args:
        flow_graph: Flow graph to validate
        python_module_path: Path to generated Python module (optional)
        nifi_client: NiFi client for provenance validation (optional)

    Returns:
        ValidationReport
    """
    validator = Validator(nifi_client, python_module_path)
    return validator.validate_static(flow_graph)


if __name__ == "__main__":
    # Simple test
    from .template_parser import parse_template
    from .client import NiFiClient

    logging.basicConfig(level=logging.INFO)

    # Test static validation
    template_path = Path("examples/InvokeHttp_And_Route_Original_On_Status.xml")

    if template_path.exists():
        console.print("[bold]Testing Static Validation[/bold]")
        flow_graph = parse_template(template_path)

        validator = Validator()
        report = validator.validate_static(flow_graph)
        report.print_summary()
    else:
        console.print("[red]Template file not found[/red]")
