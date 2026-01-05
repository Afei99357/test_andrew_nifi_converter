#!/usr/bin/env python3
"""
Validate generated code against external system data

Use this when:
- NiFi sends data to external systems (databases, APIs, files)
- Provenance content is not available
- You want to validate end-to-end results

Example scenarios:
- NiFi → PutSQL → Database (compare with DB)
- NiFi → PutFile → HDFS (compare with files)
- NiFi → InvokeHTTP → API (compare with API logs)
"""

import sys
sys.path.insert(0, '..')

from generated.generated_from_provenance import *
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


def example_database_validation():
    """
    Example: Validate against database

    Scenario: NiFi flow writes to PostgreSQL, we want to verify
    generated Python produces same results
    """
    console.print(Panel.fit(
        "[bold cyan]External System Validation - Database[/bold cyan]",
        border_style="cyan"
    ))

    # Step 1: Get data from external system (database)
    console.print("\n[yellow]1. Fetching data from external system (simulated)...[/yellow]")

    # Simulated database query results
    # In real usage, you'd do: SELECT * FROM output_table WHERE timestamp > ...
    db_records = [
        {'id': 1, 'url': 'https://apache.org/', 'timestamp': '2026-01-03 22:00:00'},
        {'id': 2, 'url': 'https://spark.apache.org/', 'timestamp': '2026-01-03 22:01:00'},
        {'id': 3, 'url': 'https://kafka.apache.org/', 'timestamp': '2026-01-03 22:02:00'},
    ]

    console.print(f"[green]✓[/green] Retrieved {len(db_records)} records from database")

    # Step 2: Run same input through generated Python
    console.print("\n[yellow]2. Running generated Python code...[/yellow]")

    # Simulate running the flow with same source data
    test_content = b"""
    Visit https://apache.org/
    Check out https://spark.apache.org/
    See https://kafka.apache.org/
    """

    test_ff = FlowFile(content=test_content)

    # Execute path that extracts URLs
    result = execute_path_1(test_ff)  # ExtractText → DetectDuplicate

    console.print(f"[green]✓[/green] Generated code executed")

    # Step 3: Compare results
    console.print("\n[yellow]3. Comparing results...[/yellow]")

    # Extract URLs from Python output
    python_urls = []
    if result:
        for ff in result:
            if 'url' in ff.attributes:
                python_urls.append(ff.attributes['url'])

    # Extract URLs from database
    db_urls = [r['url'] for r in db_records]

    # Compare
    matched = len(set(python_urls) & set(db_urls))
    total_db = len(db_urls)
    total_python = len(python_urls)

    console.print(f"\n[cyan]Comparison Results:[/cyan]")
    console.print(f"  Database records: {total_db}")
    console.print(f"  Python results: {total_python}")
    console.print(f"  Matched: [green]{matched}[/green]")
    console.print(f"  Match rate: [bold]{matched/total_db*100:.1f}%[/bold]")

    # Detailed comparison
    table = Table(title="URL Comparison")
    table.add_column("URL", style="cyan")
    table.add_column("In Database", justify="center", style="green")
    table.add_column("In Python", justify="center", style="yellow")

    all_urls = set(db_urls + python_urls)
    for url in list(all_urls)[:10]:
        in_db = "✓" if url in db_urls else "✗"
        in_python = "✓" if url in python_urls else "✗"
        table.add_row(url[:50] + "..." if len(url) > 50 else url, in_db, in_python)

    console.print("\n")
    console.print(table)

    # Assessment
    console.print("\n" + "=" * 80)
    if matched == total_db:
        console.print("[bold green]✓ Perfect match! Generated code produces same results as NiFi[/bold green]")
    elif matched / total_db >= 0.8:
        console.print(f"[yellow]⚠ Good match ({matched/total_db*100:.1f}%), minor differences[/yellow]")
    else:
        console.print(f"[red]✗ Significant differences ({matched/total_db*100:.1f}% match)[/red]")
    console.print("=" * 80)


def example_file_validation():
    """
    Example: Validate against files

    Scenario: NiFi flow writes to files (PutFile/PutHDFS),
    we compare generated Python output with actual files
    """
    console.print("\n\n")
    console.print(Panel.fit(
        "[bold cyan]External System Validation - Files[/bold cyan]",
        border_style="cyan"
    ))

    console.print("\n[cyan]Example scenario:[/cyan]")
    console.print("  1. Read files from NiFi output directory:")
    console.print("     $ ls /nifi/output/*.txt")
    console.print("")
    console.print("  2. Run generated Python and save to /python/output/")
    console.print("")
    console.print("  3. Compare:")
    console.print("     $ diff /nifi/output/file1.txt /python/output/file1.txt")
    console.print("")
    console.print("[green]✓[/green] If diff shows no differences, validation passes!")


def example_api_validation():
    """
    Example: Validate against API logs

    Scenario: NiFi sends data to external API,
    we compare generated Python API calls with actual API logs
    """
    console.print("\n\n")
    console.print(Panel.fit(
        "[bold cyan]External System Validation - API[/bold cyan]",
        border_style="cyan"
    ))

    console.print("\n[cyan]Example scenario:[/cyan]")
    console.print("  1. Get API logs from NiFi flow:")
    console.print("     $ curl https://api.example.com/logs?source=nifi")
    console.print("")
    console.print("  2. Run generated Python and capture API calls:")
    console.print("     $ python generated_code.py --log-api-calls")
    console.print("")
    console.print("  3. Compare payloads:")
    console.print("     - Same URLs called?")
    console.print("     - Same request bodies?")
    console.print("     - Same parameters?")
    console.print("")
    console.print("[green]✓[/green] If API calls match, validation passes!")


def main():
    console.print(Panel.fit(
        "[bold green]External System Validation Examples[/bold green]\n" +
        "[dim]Validate when data is sent outside NiFi[/dim]",
        border_style="green"
    ))

    # Show different validation scenarios
    example_database_validation()
    example_file_validation()
    example_api_validation()

    # Summary
    console.print("\n\n")
    console.print(Panel.fit(
        "[bold]External Validation Summary[/bold]\n\n" +
        "When provenance content is not available, validate against:\n\n" +
        "[green]✓[/green] Databases - Compare SQL results\n" +
        "[green]✓[/green] Files - Compare file contents\n" +
        "[green]✓[/green] APIs - Compare API calls/logs\n" +
        "[green]✓[/green] Message queues - Compare messages sent\n\n" +
        "[cyan]This approach:[/cyan]\n" +
        "  • Works when content repository is disabled\n" +
        "  • Validates end-to-end results\n" +
        "  • Tests real production output",
        border_style="green"
    ))


if __name__ == "__main__":
    main()
