#!/usr/bin/env python3
"""
Complete provenance-driven workflow: Walk provenance → Generate Python

This script combines:
1. walk_provenance.py - Collects provenance events and processor configs
2. generate_from_provenance.py - Generates Python code from provenance

Run this to go from NiFi provenance directly to generated Python code.
"""

from nifi2py.client import NiFiClient
from nifi2py.provenance_generator import ProvenanceDrivenGenerator
from nifi2py.processor_converters import get_converter
from nifi2py.lineage_tracer import LineageTracer
from collections import defaultdict, Counter
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
import json
from pathlib import Path
from datetime import datetime

console = Console()


def main():
    console.print(Panel.fit(
        "[bold cyan]Provenance-Driven Python Generation[/bold cyan]\n"
        "Complete workflow: NiFi Provenance → Python Code",
        border_style="cyan"
    ))

    # Configuration
    max_events = 5000  # Paginate to collect up to 5000 events
    sample_size = 20   # Number of provenance samples per processor

    # ========================================================================
    # PHASE 1: Connect to NiFi
    # ========================================================================
    console.print("\n[yellow]Phase 1:[/yellow] Connecting to NiFi...")

    client = NiFiClient(
        "https://127.0.0.1:8443/nifi",
        username="apsaltis",
        password="deltalakeforthewin",
        verify_ssl=False
    )
    console.print("[green]✓[/green] Connected successfully")

    # ========================================================================
    # PHASE 2: Query Provenance (with pagination)
    # ========================================================================
    console.print(f"\n[yellow]Phase 2:[/yellow] Querying provenance repository (max {max_events} events)...")

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Fetching provenance events...", total=None)
            events = client.query_provenance(max_events=max_events)
            progress.update(task, completed=True)

        console.print(f"[green]✓[/green] Found {len(events)} provenance events")
    except Exception as e:
        console.print(f"[red]✗[/red] Provenance query failed: {e}")
        console.print("\n[yellow]Note:[/yellow] Make sure processors are running in NiFi")
        client.close()
        return

    if not events:
        console.print("\n[yellow]No provenance events found.[/yellow]")
        console.print("Please start some processors in NiFi and wait a few minutes.")
        client.close()
        return

    # ========================================================================
    # PHASE 3: Group Events by Processor
    # ========================================================================
    console.print("\n[yellow]Phase 3:[/yellow] Grouping events by processor...")

    events_by_processor = defaultdict(list)
    for event in events:
        processor_id = event.get('componentId', 'unknown')
        events_by_processor[processor_id].append(event)

    console.print(f"[green]✓[/green] Found {len(events_by_processor)} unique processors")

    # Display processor activity table
    table = Table(title="Processor Activity from Provenance")
    table.add_column("Processor ID", style="cyan")
    table.add_column("Event Count", justify="right", style="green")
    table.add_column("Event Types", style="yellow")
    table.add_column("Component Type", style="magenta")

    for processor_id, proc_events in sorted(events_by_processor.items(), key=lambda x: len(x[1]), reverse=True)[:10]:
        event_types = Counter(e.get('eventType', 'UNKNOWN') for e in proc_events)
        event_type_str = ", ".join(f"{t}({c})" for t, c in event_types.most_common(3))
        component_type = proc_events[0].get('componentType', 'Unknown')
        table.add_row(
            processor_id[:16] + "...",
            str(len(proc_events)),
            event_type_str,
            component_type
        )

    console.print(table)

    # ========================================================================
    # PHASE 4: Fetch Processor Configurations
    # ========================================================================
    console.print("\n[yellow]Phase 4:[/yellow] Fetching processor configurations...")

    processor_configs = {}
    failed_processors = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task(
            f"Fetching configs for {len(events_by_processor)} processors...",
            total=len(events_by_processor)
        )

        for processor_id in events_by_processor.keys():
            try:
                config = client.get_processor(processor_id)
                processor_configs[processor_id] = config
                progress.advance(task)
            except Exception as e:
                failed_processors.append((processor_id, str(e)))
                progress.advance(task)

    console.print(f"[green]✓[/green] Fetched {len(processor_configs)} processor configs")
    if failed_processors:
        console.print(f"[yellow]⚠[/yellow]  {len(failed_processors)} processors failed (may be deleted)")

    # ========================================================================
    # PHASE 5: Generate Python Code
    # ========================================================================
    console.print("\n[yellow]Phase 5:[/yellow] Generating Python code from provenance...")

    generator = ProvenanceDrivenGenerator(client)

    # Generate code for each processor
    generated_code = []
    generated_code.append("#!/usr/bin/env python3")
    generated_code.append('"""')
    generated_code.append("Generated Python code from NiFi provenance data")
    generated_code.append(f"Generated at: {datetime.now().isoformat()}")
    generated_code.append(f"Total processors: {len(processor_configs)}")
    generated_code.append(f"Total provenance events: {len(events)}")
    generated_code.append('"""')
    generated_code.append("")
    generated_code.append("from typing import Dict, List, Any")
    generated_code.append("from datetime import datetime")
    generated_code.append("import re")
    generated_code.append("")
    generated_code.append("")
    generated_code.append("class FlowFile:")
    generated_code.append("    def __init__(self, content: bytes = b'', attributes: Dict[str, str] = None):")
    generated_code.append("        self.content = content")
    generated_code.append("        self.attributes = attributes or {}")
    generated_code.append("")
    generated_code.append("")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task(
            f"Generating Python for {len(processor_configs)} processors...",
            total=len(processor_configs)
        )

        for processor_id, config in processor_configs.items():
            try:
                component = config['component']
                proc_name = component['name']
                proc_type = component['type'].split('.')[-1]

                # Get provenance samples for this processor
                proc_events = events_by_processor[processor_id][:sample_size]

                # Try to use processor-specific converter
                converter = get_converter(config, proc_events)

                if converter:
                    # Generate functional code using converter
                    function_code = converter.generate_function()
                    generated_code.append(function_code)
                    generated_code.append("")
                    generated_code.append("")
                else:
                    # Fallback to stub for unsupported processors
                    func_name = f"process_{proc_type.lower()}_{processor_id.replace('-', '_')[:16]}"
                    generated_code.append(f"def {func_name}(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:")
                    generated_code.append(f'    """')
                    generated_code.append(f"    {proc_type}: {proc_name}")
                    generated_code.append(f"    TODO: No converter available for {proc_type}")
                    generated_code.append(f"    Based on {len(proc_events)} provenance samples")
                    generated_code.append(f'    """')
                    generated_code.append(f"    # Observed event types: {set(e.get('eventType') for e in proc_events)}")
                    generated_code.append(f"    return {{'success': [flowfile]}}")
                    generated_code.append("")
                    generated_code.append("")

                progress.advance(task)
            except Exception as e:
                console.print(f"[yellow]⚠[/yellow]  Failed to generate code for {processor_id}: {e}")
                progress.advance(task)

    # ========================================================================
    # PHASE 5.5: Generate Workflow Execution from Lineage
    # ========================================================================
    console.print("\n[yellow]Phase 5.5:[/yellow] Analyzing FlowFile lineage...")

    # Create lineage tracer
    tracer = LineageTracer(events)

    # Get execution paths
    execution_paths = tracer.get_execution_paths()
    console.print(f"[green]✓[/green] Found {len(execution_paths)} execution paths")

    # Build processor ID to function name mapping
    proc_id_to_func = {}
    for processor_id, config in processor_configs.items():
        component = config['component']
        proc_type = component['type'].split('.')[-1]
        func_name = f"process_{proc_type.lower()}_{processor_id.replace('-', '_')[:16]}"
        proc_id_to_func[processor_id] = func_name

    # Generate workflow execution code
    generated_code.append("")
    generated_code.append("# " + "=" * 76)
    generated_code.append("# Workflow Execution (Generated from FlowFile Lineage)")
    generated_code.append("# " + "=" * 76)
    generated_code.append("")

    # Generate execution function for each path
    for path_idx, path in enumerate(execution_paths[:5]):  # Limit to first 5 paths
        console.print(f"[cyan]•[/cyan] Path {path_idx + 1}: {' → '.join([p[1] for p in path[:3]])}{('...' if len(path) > 3 else '')}")

        generated_code.append(f"def execute_path_{path_idx + 1}(initial_flowfile: FlowFile = None):")
        generated_code.append(f'    """')
        generated_code.append(f'    Execute Path {path_idx + 1} based on observed FlowFile lineage')
        generated_code.append(f'    ')
        generated_code.append(f'    Processor sequence:')
        for proc_id, proc_type, proc_name in path[:10]:  # Show first 10
            generated_code.append(f'    - {proc_type}: {proc_name}')
        if len(path) > 10:
            generated_code.append(f'    ... and {len(path) - 10} more processors')
        generated_code.append(f'    """')
        generated_code.append(f'    ')
        generated_code.append(f'    # Initialize state')
        generated_code.append(f'    cache = set()  # For DetectDuplicate processors')
        generated_code.append(f'    ')

        # Generate call sequence
        generated_code.append(f'    # Execute processor sequence')
        flowfile_var = "initial_flowfile if initial_flowfile else FlowFile()"

        for i, (proc_id, proc_type, proc_name) in enumerate(path):
            func_name = proc_id_to_func.get(proc_id)

            if not func_name:
                # Processor not in our configs (might be deleted)
                generated_code.append(f'    # Skipping {proc_type}: {proc_name} (no config available)')
                continue

            if i == 0:
                # First processor
                if "DetectDuplicate" in proc_type:
                    generated_code.append(f'    result = {func_name}({flowfile_var}, cache)')
                else:
                    generated_code.append(f'    result = {func_name}({flowfile_var})')
            else:
                # Subsequent processors - use output from previous
                if "DetectDuplicate" in proc_type:
                    generated_code.append(f'    result = {func_name}(flowfiles[0], cache) if flowfiles else {{"success": []}}')
                else:
                    generated_code.append(f'    result = {func_name}(flowfiles[0]) if flowfiles else {{"success": []}}')

            generated_code.append(f'    ')
            generated_code.append(f'    # Get FlowFiles from first relationship (usually success or matched)')
            generated_code.append(f'    flowfiles = list(result.values())[0] if result else []')
            generated_code.append(f'    ')

        generated_code.append(f'    return flowfiles')
        generated_code.append("")
        generated_code.append("")

    # ========================================================================
    # PHASE 6: Save Results
    # ========================================================================
    console.print("\n[yellow]Phase 6:[/yellow] Saving results...")

    # Save generated Python code
    output_file = Path("generated_from_provenance.py")
    output_file.write_text("\n".join(generated_code))
    console.print(f"[green]✓[/green] Generated Python code: {output_file}")

    # Save provenance analysis
    analysis = {
        "generated_at": datetime.now().isoformat(),
        "total_events": len(events),
        "unique_processors": len(events_by_processor),
        "processor_configs": len(processor_configs),
        "failed_processors": len(failed_processors),
        "event_summary": {
            pid: {
                "count": len(evts),
                "types": dict(Counter(e.get('eventType', 'UNKNOWN') for e in evts)),
                "component_type": evts[0].get('componentType', 'Unknown'),
                "component_name": evts[0].get('componentName', 'Unknown')
            }
            for pid, evts in events_by_processor.items()
        }
    }

    analysis_file = Path("provenance_analysis.json")
    analysis_file.write_text(json.dumps(analysis, indent=2))
    console.print(f"[green]✓[/green] Provenance analysis: {analysis_file}")

    # ========================================================================
    # Summary
    # ========================================================================
    console.print("\n" + "="*60)
    console.print("[bold green]Generation Complete![/bold green]")
    console.print("="*60)
    console.print(f"\n📊 [cyan]Statistics:[/cyan]")
    console.print(f"   • Total provenance events: {len(events)}")
    console.print(f"   • Unique processors: {len(events_by_processor)}")
    console.print(f"   • Configs fetched: {len(processor_configs)}")
    console.print(f"   • Python functions generated: {len(processor_configs)}")

    console.print(f"\n📁 [cyan]Output Files:[/cyan]")
    console.print(f"   • {output_file} - Generated Python code")
    console.print(f"   • {analysis_file} - Provenance analysis data")

    console.print(f"\n🎯 [cyan]Next Steps:[/cyan]")
    console.print(f"   1. Review generated code:")
    console.print(f"      cat {output_file}")
    console.print(f"")
    console.print(f"   2. Test the generated workflow execution:")
    console.print(f"      python run_generated_flow.py")
    console.print(f"")
    console.print(f"   3. Or test individual paths:")
    console.print(f"      python -c 'from generated_from_provenance import *; print(execute_path_1())'")
    console.print(f"")
    console.print(f"   4. Review discovered execution paths:")
    console.print(f"      • Found {len(execution_paths)} paths from FlowFile lineage")
    for i, path in enumerate(execution_paths[:3]):
        path_str = ' → '.join([p[1] for p in path[:3]])
        if len(path) > 3:
            path_str += '...'
        console.print(f"      • Path {i+1}: {path_str}")

    client.close()


if __name__ == "__main__":
    main()
