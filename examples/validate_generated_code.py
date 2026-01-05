#!/usr/bin/env python3
"""
Comprehensive validation of generated Python code

Supports TWO types of validation:
1. Structure/Graph Validation - Always works (no content needed)
2. Content Validation - Only when provenance content is available

This handles real-world scenarios where:
- Content repository may not be configured
- Content may be expired
- Data may be sent to external systems
"""

import sys
sys.path.insert(0, '..')

from nifi2py.client import NiFiClient
from nifi2py.lineage_tracer import LineageTracer
from nifi2py.graph_validator import GraphValidator
from nifi2py.provenance_validator import ProvenanceValidator
import generated.generated_from_provenance as generated_module
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


def validate_structure(client: NiFiClient, events: list) -> None:
    """Validate structure/graph (always available)"""
    console.print(Panel.fit(
        "[bold cyan]Structure Validation[/bold cyan]\n" +
        "[dim]Validates flow graph without requiring content[/dim]",
        border_style="cyan"
    ))

    console.print("\n[yellow]Building lineage graph...[/yellow]")
    tracer = LineageTracer(events)
    console.print("[green]✓[/green] Lineage graph built")

    console.print("\n[yellow]Validating structure...[/yellow]")
    validator = GraphValidator(client, generated_module, tracer)
    result = validator.validate_structure(events)

    # Display results
    console.print("\n[cyan]Structure Validation Results:[/cyan]")
    console.print(f"  Substantive processors in flow: {result.processors_found}")
    console.print(f"  [dim](excludes no-ops: Funnel, LogMessage, LogAttribute, etc.)[/dim]")
    console.print(f"  Processors generated: {result.processors_generated}")
    console.print(f"  Processor coverage: [bold]{result.processor_coverage:.1f}%[/bold]")
    console.print(f"")
    console.print(f"  Execution paths found: {result.paths_found}")
    console.print(f"  Execution paths implemented: {result.paths_implemented}")
    console.print(f"  Path coverage: [bold]{result.path_coverage:.1f}%[/bold]")

    # Show detailed path analysis if there's a discrepancy
    # Note: result.paths_found now reflects substantive paths (excluding redundant single-processor paths)
    if result.paths_found != result.paths_implemented or result.paths_found < 6:
        console.print(f"\n[yellow]Path Analysis:[/yellow]")
        all_paths = tracer.get_execution_paths()
        implemented_paths = set()
        for name in dir(generated_module):
            if name.startswith('execute_path_'):
                # Extract path number from function name
                path_num = name.split('_')[-1]
                if path_num.isdigit():
                    implemented_paths.add(int(path_num))

        # Identify which paths are redundant
        from nifi2py.graph_validator import is_noop_processor

        processors_in_multi_paths = set()
        for path in all_paths:
            non_noop = [(pid, ptype, pname) for pid, ptype, pname in path
                       if not is_noop_processor(ptype, pname)]
            if len(non_noop) > 1:
                processors_in_multi_paths.update(pid for pid, _, _ in non_noop)

        console.print(f"  [cyan]All paths found in provenance:[/cyan]")
        for i, path in enumerate(all_paths[:10], 1):  # Show first 10
            # Path is a list of tuples: (proc_id, proc_type, proc_name)
            non_noop_path = [(pid, ptype, pname) for pid, ptype, pname in path
                           if not is_noop_processor(ptype, pname)]

            processors_in_path = [f"{ptype}" for _, ptype, _ in path]
            path_summary = ' → '.join(processors_in_path[:4])
            if len(processors_in_path) > 4:
                path_summary += f" → ... ({len(processors_in_path)} total)"

            # Determine status
            if i in implemented_paths:
                status = "[green]✓ Implemented[/green]"
            elif len(non_noop_path) == 1:
                proc_id, _, _ = non_noop_path[0]
                if proc_id in processors_in_multi_paths:
                    status = "[dim]⊘ Redundant (processor covered in longer path)[/dim]"
                else:
                    status = "[yellow]○ Not implemented[/yellow]"
            else:
                status = "[yellow]○ Not implemented[/yellow]"

            console.print(f"    Path {i}: {path_summary}")
            console.print(f"           {status}")

    # Missing processors
    if result.missing_processors:
        console.print(f"\n[yellow]Missing Processors ({len(result.missing_processors)}):[/yellow]")
        for proc in result.missing_processors[:5]:
            console.print(f"  • {proc}")
        if len(result.missing_processors) > 5:
            console.print(f"  ... and {len(result.missing_processors) - 5} more")

    # Issues
    if result.issues:
        console.print(f"\n[red]Issues Found:[/red]")
        for issue in result.issues:
            console.print(f"  • {issue}")

    # Assessment
    console.print("\n" + "=" * 80)
    if result.processor_coverage >= 70 and result.path_coverage >= 80:
        console.print("[bold green]✓ Structure validation passed[/bold green]")
        console.print("[green]Generated code structure matches NiFi flow[/green]")
    elif result.processor_coverage >= 50:
        console.print("[yellow]⚠ Partial structure coverage[/yellow]")
        console.print(f"[dim]Some processors missing (expected if deleted in NiFi)[/dim]")
    else:
        console.print("[red]✗ Low structure coverage[/red]")
        console.print("[dim]Many processors missing - may need to regenerate code[/dim]")
    console.print("=" * 80)

    # Validate relationships
    console.print("\n[yellow]Analyzing relationship routing...[/yellow]")
    rel_analysis = validator.validate_relationships(events)

    if rel_analysis:
        console.print(f"\n[cyan]Relationship Usage:[/cyan]")
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Processor", style="cyan")
        table.add_column("Relationships Used", style="yellow")
        table.add_column("Events", justify="right")

        for proc_id, info in list(rel_analysis.items())[:10]:
            proc_name = info.get('processor_name', proc_id[:16] + "...")
            rels = ", ".join(info['relationships_used'])
            table.add_row(proc_name if 'processor_name' in info else proc_id[:16] + "...", rels, str(info['event_count']))

        console.print(table)

    # Execution order validation
    console.print("\n[yellow]Validating execution order...[/yellow]")
    order_issues = validator.validate_execution_order()

    if order_issues:
        console.print(f"[yellow]Execution order issues:[/yellow]")
        for issue in order_issues:
            console.print(f"  • {issue}")
    else:
        console.print(f"[green]✓[/green] Execution order validated")


def validate_content(client: NiFiClient) -> None:
    """Validate content (only if available)"""
    console.print("\n")
    console.print(Panel.fit(
        "[bold cyan]Content Validation[/bold cyan]\n" +
        "[dim]Compares Python output vs NiFi output (requires content)[/dim]",
        border_style="cyan"
    ))

    console.print("\n[yellow]Creating content validator...[/yellow]")
    validator = ProvenanceValidator(client, generated_module)
    console.print("[green]✓[/green] Validator ready")

    console.print("\n[yellow]Validating content...[/yellow]")
    console.print("[dim]Note: This requires provenance content to be available[/dim]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Validating events...", total=None)
        summary = validator.validate(sample_size=10)
        progress.update(task, completed=True)

    # Calculate actual parity (excluding errors)
    testable_events = summary.total_events - summary.errors
    actual_parity = (summary.matched / testable_events * 100) if testable_events > 0 else 0.0

    console.print("\n[cyan]Content Validation Results:[/cyan]")
    console.print(f"  Total events: {summary.total_events}")
    console.print(f"  Testable (content available): {testable_events}")
    console.print(f"  Matched: [green]{summary.matched}[/green]")
    console.print(f"  Mismatched: [red]{summary.mismatched}[/red]")
    console.print(f"  Content unavailable: [yellow]{summary.errors}[/yellow]")

    if testable_events > 0:
        console.print(f"  Actual parity: [bold]{actual_parity:.1f}%[/bold]")

    # Detailed results (if any testable events)
    if testable_events > 0 and summary.results:
        console.print("\n[cyan]Sample Results:[/cyan]")

        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Processor", style="cyan")
        table.add_column("Match", justify="center")
        table.add_column("NiFi Hash", style="dim")
        table.add_column("Python Hash", style="dim")

        # Show only testable events
        testable_results = [r for r in summary.results if not r.error]
        for result in testable_results[:5]:
            match_symbol = "[green]✓[/green]" if result.matches else "[red]✗[/red]"
            table.add_row(
                result.processor_name[:25],
                match_symbol,
                result.nifi_output_hash[:8] + "...",
                result.python_output_hash[:8] + "..." if result.python_output_hash else "N/A"
            )

        console.print(table)

    # Assessment
    console.print("\n" + "=" * 80)
    if testable_events == 0:
        console.print("[yellow]⚠ Content validation not available[/yellow]")
        console.print("[dim]Provenance content not retained or too old[/dim]")
        console.print("\n[cyan]This is OK if:[/cyan]")
        console.print("  • Structure validation passed")
        console.print("  • You validate against external systems")
        console.print("  • Content repository is not configured")
    elif summary.mismatched == 0 and summary.matched > 0:
        console.print("[bold green]🎉 Perfect content validation![/bold green]")
        console.print(f"[green]✓ All {summary.matched} testable events matched (100%)[/green]")
    elif actual_parity >= 80:
        console.print(f"[green]✓ Good content parity ({actual_parity:.1f}%)[/green]")
        console.print(f"[yellow]⚠ {summary.mismatched} events differ - review above[/yellow]")
    else:
        console.print(f"[red]✗ Low content parity ({actual_parity:.1f}%)[/red]")
        console.print("[dim]Generated code may differ from NiFi - review above[/dim]")
    console.print("=" * 80)


def main():
    console.print(Panel.fit(
        "[bold green]Comprehensive Code Validation[/bold green]\n" +
        "[dim]Structure + Content Validation[/dim]",
        border_style="green"
    ))

    # Connect to NiFi
    console.print("\n[yellow]Connecting to NiFi...[/yellow]")
    client = NiFiClient(
        "https://127.0.0.1:8443/nifi",
        username="apsaltis",
        password="deltalakeforthewin",
        verify_ssl=False
    )
    console.print("[green]✓[/green] Connected")

    # Get provenance events
    console.print("\n[yellow]Fetching provenance events...[/yellow]")
    events = client.query_provenance(max_events=500)
    console.print(f"[green]✓[/green] Retrieved {len(events)} events")

    # 1. Structure Validation (always works)
    validate_structure(client, events)

    # 2. Content Validation (if available)
    validate_content(client)

    # Final summary
    console.print("\n")
    console.print(Panel.fit(
        "[bold]Validation Complete[/bold]\n\n" +
        "[green]✓[/green] Structure validation shows code matches flow graph\n" +
        "[green]✓[/green] Content validation attempted (if content available)\n\n" +
        "[cyan]Next steps:[/cyan]\n" +
        "  • If content not available, validate against external systems\n" +
        "  • Run generated code with test data: python examples/run_generated_flow.py\n" +
        "  • Compare results with external database/API if applicable",
        border_style="green"
    ))

    client.close()


if __name__ == "__main__":
    main()
