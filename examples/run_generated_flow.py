#!/usr/bin/env python3
"""
Comprehensive test of all generated workflow execution paths
"""

import sys
sys.path.insert(0, '..')

from generated.generated_from_provenance import *
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()

def print_flowfile_details(flowfiles, title="FlowFiles"):
    """Pretty print FlowFile details"""
    if not flowfiles:
        console.print(f"[yellow]{title}:[/yellow] None")
        return

    console.print(f"\n[cyan]{title}:[/cyan] {len(flowfiles)} FlowFile(s)")
    for i, ff in enumerate(flowfiles[:3]):  # Show first 3
        console.print(f"  [{i+1}]")
        console.print(f"    Content length: {len(ff.content)} bytes")
        if ff.attributes:
            console.print(f"    Attributes:")
            for key, value in list(ff.attributes.items())[:5]:
                value_display = value[:50] + "..." if len(str(value)) > 50 else value
                console.print(f"      {key}: {value_display}")

    if len(flowfiles) > 3:
        console.print(f"  ... and {len(flowfiles) - 3} more")


def test_path_1():
    """Test Path 1: ExtractText → DetectDuplicate"""
    console.print(Panel.fit(
        "[bold cyan]Path 1: ExtractText → DetectDuplicate[/bold cyan]",
        border_style="cyan"
    ))

    # Create test data with URLs
    test_content = b"""
    Check out these Apache projects:
    Visit https://www.apache.org/foundation/ for more info
    Also see https://spark.apache.org/ for Spark
    And https://kafka.apache.org/ for Kafka
    Duplicate: https://www.apache.org/foundation/ (again!)
    """

    console.print("\n[yellow]Input FlowFile:[/yellow]")
    console.print(f"  Content: {len(test_content)} bytes")
    console.print(f"  Preview: {test_content[:100].decode()}...")

    test_ff = FlowFile(content=test_content)

    console.print("\n[yellow]Executing Path 1...[/yellow]")
    result = execute_path_1(test_ff)

    print_flowfile_details(result, "Output")

    return len(result) > 0


def test_path_2():
    """Test Path 2: RouteText → SplitText"""
    console.print(Panel.fit(
        "[bold cyan]Path 2: RouteText → SplitText[/bold cyan]",
        border_style="cyan"
    ))

    # Create test data with URLs
    test_content = b"""Line 1: https://apache.org/
Line 2: https://spark.apache.org/
Line 3: https://kafka.apache.org/
Line 4: Some text without URL"""

    console.print("\n[yellow]Input FlowFile:[/yellow]")
    console.print(f"  Content: {len(test_content)} bytes")
    console.print(f"  Lines: {len(test_content.splitlines())}")

    test_ff = FlowFile(content=test_content)

    console.print("\n[yellow]Executing Path 2...[/yellow]")
    result = execute_path_2(test_ff)

    print_flowfile_details(result, "Output (Split Lines)")

    return len(result) > 0


def test_path_5():
    """Test Path 5: GetHTTP → Funnel → RouteText (Complete web crawler)"""
    console.print(Panel.fit(
        "[bold cyan]Path 5: GetHTTP → Funnel → RouteText[/bold cyan]\n" +
        "[dim]Complete web crawler workflow[/dim]",
        border_style="cyan"
    ))

    console.print("\n[yellow]Executing Path 5...[/yellow]")
    console.print("  [dim]This will fetch http://www.apache.org/[/dim]")

    try:
        result = execute_path_5()

        if result:
            console.print(f"\n[green]✓[/green] Execution completed successfully")
            print_flowfile_details(result, "Output")

            # Show content preview
            if result[0].content:
                preview = result[0].content[:200].decode('utf-8', errors='ignore')
                console.print(f"\n[yellow]Content Preview:[/yellow]")
                console.print(f"  {preview}...")
        else:
            console.print(f"\n[red]✗[/red] No FlowFiles returned")
            return False

        return True
    except Exception as e:
        console.print(f"\n[red]✗[/red] Execution failed: {e}")
        return False


def test_all_paths():
    """Test all available execution paths"""
    console.print("\n" + "=" * 80)
    console.print("[bold green]Testing All Generated Workflow Execution Paths[/bold green]")
    console.print("=" * 80 + "\n")

    results = {}

    # Test each path
    paths_to_test = [
        ("Path 1", test_path_1),
        ("Path 2", test_path_2),
        ("Path 5", test_path_5),
    ]

    for path_name, test_func in paths_to_test:
        try:
            console.print("\n")
            success = test_func()
            results[path_name] = "✓ PASS" if success else "✗ FAIL"
        except Exception as e:
            console.print(f"\n[red]Exception:[/red] {e}")
            results[path_name] = f"✗ ERROR: {str(e)[:50]}"

        console.print("\n" + "-" * 80)

    # Summary table
    console.print("\n")
    console.print(Panel.fit(
        "[bold]Test Summary[/bold]",
        border_style="green"
    ))

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Path", style="cyan")
    table.add_column("Result", style="green")

    for path_name, result in results.items():
        style = "green" if "PASS" in result else "red"
        table.add_row(path_name, f"[{style}]{result}[/{style}]")

    console.print(table)

    # Overall result
    passed = sum(1 for r in results.values() if "PASS" in r)
    total = len(results)

    console.print(f"\n[bold]Overall:[/bold] {passed}/{total} paths passed")

    if passed == total:
        console.print("[green]🎉 All tests passed![/green]")
    else:
        console.print("[yellow]⚠ Some tests failed - review output above[/yellow]")


def main():
    console.print(Panel.fit(
        "[bold cyan]Generated Workflow Testing Suite[/bold cyan]\n" +
        "[dim]Testing Python code generated from NiFi provenance[/dim]",
        border_style="cyan"
    ))

    test_all_paths()

    console.print("\n" + "=" * 80)
    console.print("[bold]Testing Complete![/bold]")
    console.print("=" * 80)

    console.print("\n[cyan]Next steps:[/cyan]")
    console.print("  • Review the generated code: cat generated/generated_from_provenance.py")
    console.print("  • Check provenance analysis: cat generated/provenance_analysis.json")
    console.print("  • Run validation: python examples/validate_against_nifi.py")
    console.print("")


if __name__ == "__main__":
    main()
