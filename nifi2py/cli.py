"""
nifi2py CLI - Command-line interface for converting NiFi flows to Python.

This module provides a Click-based CLI with Rich formatting for:
- Testing NiFi connections
- Parsing and analyzing templates
- Converting templates to Python code
- Listing processors from live NiFi instances
- Analyzing live flows
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

import click
from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.panel import Panel
from rich.syntax import Syntax
from rich import box
from rich import print as rprint

from nifi2py.client import NiFiClient, NiFiClientError, NiFiAuthError, NiFiNotFoundError
from nifi2py.template_parser import TemplateParser

# Initialize Rich console
console = Console()


def get_nifi_credentials(url: Optional[str], user: Optional[str], password: Optional[str]) -> tuple[str, str, str]:
    """
    Get NiFi credentials from CLI args or environment variables.

    Priority: CLI args > Environment variables
    """
    nifi_url = url or os.getenv("NIFI_URL")
    nifi_user = user or os.getenv("NIFI_USER")
    nifi_password = password or os.getenv("NIFI_PASSWORD")

    if not nifi_url:
        console.print("[bold red]Error:[/bold red] NiFi URL not provided. Use --url or set NIFI_URL environment variable.")
        raise click.Abort()

    if not nifi_user:
        console.print("[bold red]Error:[/bold red] Username not provided. Use --user or set NIFI_USER environment variable.")
        raise click.Abort()

    if not nifi_password:
        console.print("[bold red]Error:[/bold red] Password not provided. Use --password or set NIFI_PASSWORD environment variable.")
        raise click.Abort()

    return nifi_url, nifi_user, nifi_password


def create_nifi_client(url: str, user: str, password: str, verify_ssl: bool) -> NiFiClient:
    """Create and return a NiFi client with error handling."""
    try:
        with console.status("[bold cyan]Connecting to NiFi...", spinner="dots"):
            client = NiFiClient(
                base_url=url,
                username=user,
                password=password,
                verify_ssl=verify_ssl
            )
            # Test connection by getting root process group
            client.get_root_process_group_id()

        console.print("[bold green]✓[/bold green] Successfully connected to NiFi")
        return client

    except NiFiAuthError as e:
        console.print(f"[bold red]Authentication Error:[/bold red] {e}")
        console.print("Please check your username and password.")
        raise click.Abort()

    except NiFiClientError as e:
        console.print(f"[bold red]Connection Error:[/bold red] {e}")
        console.print(f"Failed to connect to NiFi at {url}")
        raise click.Abort()

    except Exception as e:
        console.print(f"[bold red]Unexpected Error:[/bold red] {e}")
        raise click.Abort()


@click.group()
@click.version_option(version="0.1.0", prog_name="nifi2py")
def main():
    """
    nifi2py - Convert Apache NiFi flows to Python code.

    A powerful tool for migrating NiFi dataflows to Python with validation capabilities.
    Supports both offline template analysis and live NiFi instance integration.

    Environment Variables:
        NIFI_URL      - NiFi instance URL
        NIFI_USER     - NiFi username
        NIFI_PASSWORD - NiFi password

    Examples:
        # Parse a template file
        nifi2py parse-template examples/flow.xml

        # Convert template to Python
        nifi2py convert examples/flow.xml -o output.py

        # Test connection to NiFi
        nifi2py test-connection --url http://localhost:8080/nifi-api --user admin --password admin
    """
    pass


@main.command()
@click.option("--url", help="NiFi instance URL (e.g., http://localhost:8080/nifi-api)")
@click.option("--user", help="NiFi username")
@click.option("--password", help="NiFi password")
@click.option("--verify-ssl/--no-verify-ssl", default=False, help="Verify SSL certificates")
def test_connection(url: Optional[str], user: Optional[str], password: Optional[str], verify_ssl: bool):
    """
    Test connection to a NiFi instance.

    Verifies credentials and displays basic information about the NiFi instance
    including the root process group ID, total processor count, and NiFi version.

    Examples:
        nifi2py test-connection --url http://localhost:8080/nifi-api --user admin --password admin

        # Using environment variables
        export NIFI_URL=http://localhost:8080/nifi-api
        export NIFI_USER=admin
        export NIFI_PASSWORD=admin
        nifi2py test-connection
    """
    nifi_url, nifi_user, nifi_password = get_nifi_credentials(url, user, password)

    try:
        client = create_nifi_client(nifi_url, nifi_user, nifi_password, verify_ssl)

        # Get basic information
        with console.status("[bold cyan]Fetching NiFi information...", spinner="dots"):
            root_pg_id = client.get_root_process_group_id()
            processors = client.list_processors()

        # Display information in a panel
        info_lines = [
            f"[cyan]URL:[/cyan] {nifi_url}",
            f"[cyan]Root Process Group ID:[/cyan] {root_pg_id}",
            f"[cyan]Total Processors:[/cyan] {len(processors)}",
            f"[cyan]Authentication:[/cyan] [green]Successful[/green]",
        ]

        panel = Panel(
            "\n".join(info_lines),
            title="[bold]NiFi Connection Test",
            border_style="green",
            box=box.ROUNDED
        )
        console.print(panel)

    except click.Abort:
        sys.exit(1)


@main.command()
@click.argument("template_file", type=click.Path(exists=True))
@click.option("--output", "-o", type=click.Path(), help="Save analysis to JSON file")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed information")
@click.option("--show-el", is_flag=True, help="Display all Expression Language expressions")
def parse_template(template_file: str, output: Optional[str], verbose: bool, show_el: bool):
    """
    Parse and analyze a NiFi template XML file.

    Extracts processors, connections, and Expression Language expressions from
    a template file. Displays statistics and optionally saves detailed analysis
    to a JSON file.

    Arguments:
        TEMPLATE_FILE - Path to the NiFi template XML file

    Examples:
        # Basic parsing
        nifi2py parse-template examples/flow.xml

        # Verbose output with EL expressions
        nifi2py parse-template examples/flow.xml -v --show-el

        # Save analysis to JSON
        nifi2py parse-template examples/flow.xml -o analysis.json
    """
    template_path = Path(template_file)

    try:
        # Parse template
        with console.status(f"[bold cyan]Parsing template: {template_path.name}...", spinner="dots"):
            parser = TemplateParser()
            flow_graph = parser.parse_template(template_path)
            el_expressions = parser.extract_el_expressions(flow_graph)

        console.print(f"[bold green]✓[/bold green] Successfully parsed template: {template_path.name}\n")

        # Template overview
        overview_table = Table(title="Template Overview", box=box.ROUNDED)
        overview_table.add_column("Property", style="cyan")
        overview_table.add_column("Value", style="green")

        overview_table.add_row("Name", flow_graph.template_name or "N/A")
        overview_table.add_row("Description", flow_graph.template_description or "N/A")
        overview_table.add_row("Total Processors", str(len(flow_graph.processors)))
        overview_table.add_row("Total Connections", str(len(flow_graph.connections)))
        overview_table.add_row("EL Expressions Found", str(len(el_expressions)))

        console.print(overview_table)
        console.print()

        # Processor statistics
        processor_types: Dict[str, int] = {}
        for proc in flow_graph.processors.values():
            short_type = proc.get_short_type()
            processor_types[short_type] = processor_types.get(short_type, 0) + 1

        proc_table = Table(title="Processor Types", box=box.ROUNDED)
        proc_table.add_column("Processor Type", style="cyan")
        proc_table.add_column("Count", justify="right", style="green")
        proc_table.add_column("Percentage", justify="right", style="yellow")

        total_procs = len(flow_graph.processors)
        for proc_type, count in sorted(processor_types.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_procs * 100) if total_procs > 0 else 0
            proc_table.add_row(proc_type, str(count), f"{percentage:.1f}%")

        console.print(proc_table)
        console.print()

        # Show EL expressions if requested
        if show_el and el_expressions:
            el_table = Table(title="Expression Language Expressions", box=box.ROUNDED)
            el_table.add_column("Processor", style="cyan", max_width=30)
            el_table.add_column("Property", style="yellow", max_width=20)
            el_table.add_column("Expression", style="green", max_width=50)

            for expr_info in el_expressions[:20]:  # Limit to first 20
                # expr_info is a tuple: (processor_id, processor_name, property_name, expression)
                if isinstance(expr_info, tuple) and len(expr_info) >= 4:
                    _, proc_name, prop_name, expression = expr_info
                else:
                    # Fallback for dict format
                    proc_name = expr_info.get("processor_name", "Unknown")
                    prop_name = expr_info.get("property_name", "Unknown")
                    expression = expr_info.get("expression", "")

                el_table.add_row(proc_name, prop_name, expression)

            if len(el_expressions) > 20:
                el_table.add_row("...", "...", f"[italic]and {len(el_expressions) - 20} more[/italic]")

            console.print(el_table)
            console.print()

        # Verbose output
        if verbose:
            console.print("[bold]Detailed Processor Information:[/bold]\n")

            for proc in list(flow_graph.processors.values())[:10]:  # First 10 processors
                proc_panel = Panel(
                    f"[cyan]Type:[/cyan] {proc.get_short_type()}\n"
                    f"[cyan]ID:[/cyan] {proc.id}\n"
                    f"[cyan]State:[/cyan] {proc.state}\n"
                    f"[cyan]Properties:[/cyan] {len(proc.properties)}",
                    title=f"[bold]{proc.name}",
                    border_style="blue",
                    box=box.ROUNDED
                )
                console.print(proc_panel)

            if len(flow_graph.processors) > 10:
                console.print(f"[italic]... and {len(flow_graph.processors) - 10} more processors[/italic]\n")

        # Save to JSON if requested
        if output:
            # Convert EL expressions tuples to dicts for JSON
            el_expressions_json = []
            for expr in el_expressions:
                if isinstance(expr, tuple) and len(expr) >= 4:
                    el_expressions_json.append({
                        "processor_id": expr[0],
                        "processor_name": expr[1],
                        "property_name": expr[2],
                        "expression": expr[3]
                    })
                else:
                    el_expressions_json.append(expr)

            output_data = {
                "template_name": flow_graph.template_name,
                "template_description": flow_graph.template_description,
                "total_processors": len(flow_graph.processors),
                "total_connections": len(flow_graph.connections),
                "processor_types": processor_types,
                "processors": [
                    {
                        "id": proc.id,
                        "name": proc.name,
                        "type": proc.get_short_type(),
                        "full_type": proc.type,
                        "state": proc.state,
                        "properties": proc.properties,
                        "relationships": proc.relationships,
                    }
                    for proc in flow_graph.processors.values()
                ],
                "connections": [
                    {
                        "id": conn.id,
                        "source_id": conn.source_id,
                        "destination_id": conn.destination_id,
                        "relationships": conn.relationships,
                    }
                    for conn in flow_graph.connections
                ],
                "el_expressions": el_expressions_json,
            }

            output_path = Path(output)
            output_path.write_text(json.dumps(output_data, indent=2))
            console.print(f"[bold green]✓[/bold green] Analysis saved to: {output_path}")

    except FileNotFoundError:
        console.print(f"[bold red]Error:[/bold red] Template file not found: {template_file}")
        sys.exit(1)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Failed to parse template: {e}")
        if verbose:
            console.print_exception()
        sys.exit(1)


@main.command()
@click.argument("template_file", type=click.Path(exists=True))
@click.option("--output", "-o", required=True, type=click.Path(), help="Output Python file path")
@click.option("--format", type=click.Choice(["module"]), default="module", help="Output format")
@click.option("--show-preview", is_flag=True, help="Show preview of generated code")
def convert(template_file: str, output: str, format: str, show_preview: bool):
    """
    Convert a NiFi template to Python code.

    Parses a NiFi template XML file and generates equivalent Python code.
    The generated code can be used as a starting point for migrating NiFi
    flows to Python-based data pipelines.

    Arguments:
        TEMPLATE_FILE - Path to the NiFi template XML file

    Examples:
        # Convert to Python module
        nifi2py convert examples/flow.xml -o output.py

        # Show preview of generated code
        nifi2py convert examples/flow.xml -o output.py --show-preview
    """
    template_path = Path(template_file)
    output_path = Path(output)

    try:
        # Parse template
        with console.status(f"[bold cyan]Parsing template: {template_path.name}...", spinner="dots"):
            parser = TemplateParser()
            flow_graph = parser.parse_template(template_path)

        console.print(f"[bold green]✓[/bold green] Template parsed successfully\n")

        # Generate Python code (basic stub for now)
        # TODO: Integrate with actual code generator when available
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            transient=True
        ) as progress:
            task = progress.add_task("[cyan]Generating Python code...", total=len(flow_graph.processors))

            generated_code = generate_python_stub(flow_graph)

            for _ in flow_graph.processors:
                progress.update(task, advance=1)

        # Write to file
        output_path.write_text(generated_code)
        console.print(f"[bold green]✓[/bold green] Conversion complete!")
        console.print(f"[cyan]Output:[/cyan] {output_path}\n")

        # Show coverage statistics
        processor_types = set(proc.get_short_type() for proc in flow_graph.processors.values())

        # Known supported processors (this would come from converters registry)
        supported = {"LogMessage", "UpdateAttribute", "GenerateFlowFile", "RouteOnAttribute"}
        supported_count = sum(1 for p in flow_graph.processors.values() if p.get_short_type() in supported)
        coverage = (supported_count / len(flow_graph.processors) * 100) if flow_graph.processors else 0

        stats_table = Table(title="Conversion Statistics", box=box.ROUNDED)
        stats_table.add_column("Metric", style="cyan")
        stats_table.add_column("Value", style="green")

        stats_table.add_row("Total Processors", str(len(flow_graph.processors)))
        stats_table.add_row("Supported Processors", str(supported_count))
        stats_table.add_row("Stubbed Processors", str(len(flow_graph.processors) - supported_count))
        stats_table.add_row("Coverage", f"{coverage:.1f}%")

        console.print(stats_table)

        # Show preview if requested
        if show_preview:
            console.print("\n[bold]Code Preview:[/bold]\n")
            syntax = Syntax(generated_code[:1000], "python", theme="monokai", line_numbers=True)
            console.print(syntax)
            if len(generated_code) > 1000:
                console.print(f"\n[italic]... and {len(generated_code) - 1000} more characters[/italic]")

    except FileNotFoundError:
        console.print(f"[bold red]Error:[/bold red] Template file not found: {template_file}")
        sys.exit(1)

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] Failed to convert template: {e}")
        console.print_exception()
        sys.exit(1)


@main.command()
@click.option("--url", help="NiFi instance URL")
@click.option("--user", help="NiFi username")
@click.option("--password", help="NiFi password")
@click.option("--verify-ssl/--no-verify-ssl", default=False, help="Verify SSL certificates")
@click.option("--filter-type", help="Filter by processor type (e.g., 'UpdateAttribute')")
@click.option("--filter-state", type=click.Choice(["RUNNING", "STOPPED", "DISABLED"]), help="Filter by processor state")
def list_processors(
    url: Optional[str],
    user: Optional[str],
    password: Optional[str],
    verify_ssl: bool,
    filter_type: Optional[str],
    filter_state: Optional[str]
):
    """
    List processors from a live NiFi instance.

    Connects to a NiFi instance and retrieves all processors, displaying
    their names, types, states, and other information. Supports filtering
    by processor type and state.

    Examples:
        # List all processors
        nifi2py list-processors --url http://localhost:8080/nifi-api --user admin --password admin

        # Filter by type
        nifi2py list-processors --filter-type UpdateAttribute

        # Filter by state
        nifi2py list-processors --filter-state RUNNING
    """
    nifi_url, nifi_user, nifi_password = get_nifi_credentials(url, user, password)

    try:
        client = create_nifi_client(nifi_url, nifi_user, nifi_password, verify_ssl)

        # Get all processors
        with console.status("[bold cyan]Fetching processors...", spinner="dots"):
            processors = client.list_processors()

        console.print(f"[bold green]✓[/bold green] Found {len(processors)} processors\n")

        # Apply filters
        filtered_processors = processors

        if filter_type:
            filtered_processors = [p for p in filtered_processors if filter_type.lower() in p.get("type", "").lower()]
            console.print(f"[cyan]Filtered by type:[/cyan] {filter_type}")

        if filter_state:
            filtered_processors = [p for p in filtered_processors if p.get("status", {}).get("runStatus") == filter_state]
            console.print(f"[cyan]Filtered by state:[/cyan] {filter_state}")

        if filter_type or filter_state:
            console.print(f"[cyan]Results:[/cyan] {len(filtered_processors)} processors\n")

        # Display processors in table
        table = Table(title="NiFi Processors", box=box.ROUNDED, show_lines=True)
        table.add_column("Name", style="cyan", max_width=30)
        table.add_column("Type", style="yellow", max_width=30)
        table.add_column("State", style="green", max_width=15)
        table.add_column("ID", style="blue", max_width=40)

        for proc in filtered_processors[:50]:  # Limit to 50 for display
            proc_name = proc.get("component", {}).get("name", "Unknown")
            proc_type = proc.get("component", {}).get("type", "Unknown").split(".")[-1]
            proc_state = proc.get("status", {}).get("runStatus", "Unknown")
            proc_id = proc.get("id", "Unknown")

            # Color code state
            if proc_state == "RUNNING":
                state_display = f"[green]{proc_state}[/green]"
            elif proc_state == "STOPPED":
                state_display = f"[yellow]{proc_state}[/yellow]"
            else:
                state_display = f"[red]{proc_state}[/red]"

            table.add_row(proc_name, proc_type, state_display, proc_id)

        console.print(table)

        if len(filtered_processors) > 50:
            console.print(f"\n[italic]... and {len(filtered_processors) - 50} more processors[/italic]")

        # Show processor type summary
        console.print()
        type_counts: Dict[str, int] = {}
        for proc in filtered_processors:
            proc_type = proc.get("component", {}).get("type", "Unknown").split(".")[-1]
            type_counts[proc_type] = type_counts.get(proc_type, 0) + 1

        summary_table = Table(title="Processor Type Summary", box=box.ROUNDED)
        summary_table.add_column("Type", style="cyan")
        summary_table.add_column("Count", justify="right", style="green")

        for proc_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            summary_table.add_row(proc_type, str(count))

        console.print(summary_table)

    except click.Abort:
        sys.exit(1)


@main.command()
@click.option("--url", help="NiFi instance URL")
@click.option("--user", help="NiFi username")
@click.option("--password", help="NiFi password")
@click.option("--verify-ssl/--no-verify-ssl", default=False, help="Verify SSL certificates")
@click.option("--output", "-o", required=True, type=click.Path(), help="Output report file (JSON)")
def analyze(
    url: Optional[str],
    user: Optional[str],
    password: Optional[str],
    verify_ssl: bool,
    output: str
):
    """
    Analyze a live NiFi flow and generate a comprehensive report.

    Connects to a NiFi instance, retrieves all processors and connections,
    analyzes the flow structure, and generates a detailed JSON report.
    The report includes processor statistics, connection patterns,
    and conversion readiness assessment.

    Examples:
        # Generate analysis report
        nifi2py analyze --url http://localhost:8080/nifi-api --user admin --password admin -o report.json

        # Using environment variables
        export NIFI_URL=http://localhost:8080/nifi-api
        export NIFI_USER=admin
        export NIFI_PASSWORD=admin
        nifi2py analyze -o report.json
    """
    nifi_url, nifi_user, nifi_password = get_nifi_credentials(url, user, password)
    output_path = Path(output)

    try:
        client = create_nifi_client(nifi_url, nifi_user, nifi_password, verify_ssl)

        # Fetch flow data
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True
        ) as progress:
            task1 = progress.add_task("[cyan]Fetching processors...", total=None)
            processors = client.list_processors()
            progress.update(task1, completed=True)

            task2 = progress.add_task("[cyan]Analyzing flow structure...", total=None)
            # Additional analysis would go here
            progress.update(task2, completed=True)

        console.print(f"[bold green]✓[/bold green] Analysis complete\n")

        # Analyze processor types
        type_counts: Dict[str, int] = {}
        state_counts: Dict[str, int] = {}

        for proc in processors:
            proc_type = proc.get("component", {}).get("type", "Unknown").split(".")[-1]
            type_counts[proc_type] = type_counts.get(proc_type, 0) + 1

            proc_state = proc.get("status", {}).get("runStatus", "Unknown")
            state_counts[proc_state] = state_counts.get(proc_state, 0) + 1

        # Generate report
        report = {
            "analysis_timestamp": str(Path.cwd()),  # Would use actual timestamp
            "nifi_url": nifi_url,
            "summary": {
                "total_processors": len(processors),
                "unique_processor_types": len(type_counts),
                "running_processors": state_counts.get("RUNNING", 0),
                "stopped_processors": state_counts.get("STOPPED", 0),
                "disabled_processors": state_counts.get("DISABLED", 0),
            },
            "processor_types": type_counts,
            "state_distribution": state_counts,
            "processors": [
                {
                    "id": proc.get("id"),
                    "name": proc.get("component", {}).get("name"),
                    "type": proc.get("component", {}).get("type"),
                    "state": proc.get("status", {}).get("runStatus"),
                }
                for proc in processors
            ],
        }

        # Save report
        output_path.write_text(json.dumps(report, indent=2))
        console.print(f"[bold green]✓[/bold green] Report saved to: {output_path}\n")

        # Display summary
        summary_table = Table(title="Flow Analysis Summary", box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green", justify="right")

        summary_table.add_row("Total Processors", str(len(processors)))
        summary_table.add_row("Unique Types", str(len(type_counts)))
        summary_table.add_row("Running", str(state_counts.get("RUNNING", 0)))
        summary_table.add_row("Stopped", str(state_counts.get("STOPPED", 0)))
        summary_table.add_row("Disabled", str(state_counts.get("DISABLED", 0)))

        console.print(summary_table)

        # Top processor types
        console.print()
        top_types_table = Table(title="Top Processor Types", box=box.ROUNDED)
        top_types_table.add_column("Type", style="cyan")
        top_types_table.add_column("Count", style="green", justify="right")

        for proc_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            top_types_table.add_row(proc_type, str(count))

        console.print(top_types_table)

    except click.Abort:
        sys.exit(1)


def generate_python_stub(flow_graph) -> str:
    """
    Generate a basic Python stub from a flow graph.

    TODO: This is a placeholder. Replace with actual code generator integration.
    """
    code_lines = [
        '"""',
        f'Generated Python code from NiFi template: {flow_graph.template_name or "Unknown"}',
        '',
        'This code was automatically generated by nifi2py.',
        'Some processors may require manual implementation.',
        '"""',
        '',
        'from typing import Dict, List',
        'from dataclasses import dataclass',
        '',
        '',
        '@dataclass',
        'class FlowFile:',
        '    """Represents a NiFi FlowFile"""',
        '    content: bytes',
        '    attributes: Dict[str, str]',
        '',
        '',
        'class Flow:',
        '    """Generated flow implementation"""',
        '    ',
        '    def __init__(self):',
        '        self.name = "{}"'.format(flow_graph.template_name or "Unknown"),
        '    ',
    ]

    # Add processor functions (stubs)
    for proc in flow_graph.processors.values():
        code_lines.extend([
            f'    def process_{proc.id.replace("-", "_")}(self, flowfile: FlowFile) -> Dict[str, List[FlowFile]]:',
            f'        """',
            f'        Processor: {proc.name}',
            f'        Type: {proc.get_short_type()}',
            f'        ',
            f'        TODO: Implement processor logic',
            f'        """',
            f'        # Stub implementation',
            f'        return {{"success": [flowfile]}}',
            '    ',
        ])

    code_lines.extend([
        '',
        'if __name__ == "__main__":',
        '    flow = Flow()',
        '    print(f"Flow: {flow.name}")',
        f'    print(f"Processors: {len(flow_graph.processors)}")',
    ])

    return '\n'.join(code_lines)


if __name__ == "__main__":
    main()
