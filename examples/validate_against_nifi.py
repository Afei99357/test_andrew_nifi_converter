#!/usr/bin/env python3
"""
Validate generated Python code against NiFi provenance data

This script compares the output of generated Python functions with
NiFi's actual output from provenance events to verify correctness.
"""

import sys
sys.path.insert(0, '..')

from nifi2py.client import NiFiClient
from nifi2py.provenance_validator import ProvenanceValidator
import generated.generated_from_provenance as generated_module
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


def main():
    console.print(Panel.fit(
        "[bold cyan]Provenance-Based Validation[/bold cyan]\n" +
        "[dim]Comparing generated Python code vs NiFi execution[/dim]",
        border_style="cyan"
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

    # Create validator
    console.print("\n[yellow]Creating validator...[/yellow]")
    validator = ProvenanceValidator(client, generated_module)
    console.print("[green]✓[/green] Validator ready")

    # Validate
    console.print("\n[yellow]Validating generated code...[/yellow]")
    console.print("[dim]Note: This requires provenance content to be available[/dim]")
    console.print("[dim]Content is only retained for a configurable period in NiFi[/dim]\n")

    try:
        summary = validator.validate(sample_size=10)

        # Display results
        console.print(Panel.fit(
            "[bold]Validation Results[/bold]",
            border_style="green" if summary.parity_percentage > 80 else "yellow"
        ))

        # Summary statistics
        console.print(f"\n[cyan]Summary:[/cyan]")
        console.print(f"  Total events validated: {summary.total_events}")
        console.print(f"  Matched: [green]{summary.matched}[/green]")
        console.print(f"  Mismatched: [red]{summary.mismatched}[/red]")
        console.print(f"  Errors: [yellow]{summary.errors}[/yellow]")
        console.print(f"  Parity: [bold]{summary.parity_percentage:.1f}%[/bold]")

        # Detailed results table
        if summary.results:
            console.print("\n[cyan]Detailed Results:[/cyan]")

            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Event ID", style="dim")
            table.add_column("Processor", style="cyan")
            table.add_column("Match", justify="center")
            table.add_column("NiFi Hash", style="yellow")
            table.add_column("Python Hash", style="yellow")
            table.add_column("Error", style="red")

            for result in summary.results[:10]:  # Show first 10
                match_symbol = "[green]✓[/green]" if result.matches else "[red]✗[/red]"
                error_display = result.error[:30] + "..." if result.error and len(result.error) > 30 else (result.error or "")

                table.add_row(
                    result.event_id[:8] + "...",
                    result.processor_name[:20],
                    match_symbol,
                    result.nifi_output_hash[:8] + "...",
                    result.python_output_hash[:8] + "..." if result.python_output_hash else "N/A",
                    error_display
                )

            console.print(table)

        # Overall assessment
        console.print("\n" + "=" * 80)

        # Calculate parity excluding errors (unavailable content)
        testable_events = summary.total_events - summary.errors
        actual_parity = (summary.matched / testable_events * 100) if testable_events > 0 else 0.0

        if testable_events == 0:
            console.print("[yellow]⚠ Cannot validate - provenance content not available[/yellow]")
            console.print("[dim]This is normal if:[/dim]")
            console.print("[dim]  • Events are too old (content expired)[/dim]")
            console.print("[dim]  • NiFi is configured not to retain content[/dim]")
            console.print("[dim]  • Processors don't have content (e.g., Funnels)[/dim]")
            console.print("\n[cyan]Tip:[/cyan] Run your NiFi flow again to generate fresh provenance data")
        elif summary.mismatched == 0 and summary.matched > 0:
            console.print("[bold green]🎉 Perfect validation! Generated code matches NiFi 100%[/bold green]")
            console.print(f"[green]✓ All {summary.matched} testable events matched perfectly[/green]")
            if summary.errors > 0:
                console.print(f"[dim]Note: {summary.errors} events had no content available (expected for old events)[/dim]")
        elif actual_parity >= 80:
            console.print(f"[green]✓ Good parity ({actual_parity:.1f}% of testable events)[/green]")
            console.print(f"[yellow]⚠ {summary.mismatched} mismatched events - review details above[/yellow]")
            if summary.errors > 0:
                console.print(f"[dim]Note: {summary.errors} events had no content available[/dim]")
        else:
            console.print(f"[red]✗ Low parity ({actual_parity:.1f}% of testable events)[/red]")
            console.print(f"[red]• {summary.mismatched} mismatched events[/red]")
            console.print("[dim]This could indicate:[/dim]")
            console.print("[dim]  • Missing processor logic[/dim]")
            console.print("[dim]  • Incorrect EL transpilation[/dim]")
            console.print("[dim]  • Different execution order[/dim]")

        console.print("=" * 80)

    except Exception as e:
        console.print(f"\n[red]Validation failed:[/red] {e}")
        console.print("\n[yellow]Common issues:[/yellow]")
        console.print("  • Provenance content not available (too old or not retained)")
        console.print("  • No recent provenance events")
        console.print("  • NiFi not configured to retain content")

    client.close()


if __name__ == "__main__":
    main()
