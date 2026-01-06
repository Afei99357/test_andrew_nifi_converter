#!/usr/bin/env python3
"""
Analyze Processor Execution Frequency for Pruning Decisions

Queries provenance events from the past 30 days for a process group
and generates a distribution plot showing how often each processor ran.

Usage:
    1. Edit TARGET_GROUP_ID and NiFi connection settings below
    2. Run: python examples/analyze_processor_usage.py

Output:
    - processor_usage_[GROUP_ID].png - Bar chart of execution counts
    - processor_usage_[GROUP_ID].csv - CSV with results
"""

from nifi2py.client import NiFiClient
from datetime import datetime, timedelta
from pathlib import Path
import csv
import matplotlib.pyplot as plt
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


def main():
    # ========================================================================
    # CONFIGURATION - UPDATE THESE!
    # ========================================================================
    TARGET_GROUP_ID = "REPLACE_WITH_YOUR_PROCESS_GROUP_ID"  # e.g., "f7f33d55-0389-1550-a325-b6af7f29d213"
    DAYS_BACK = 30  # Analyze past 30 days (can adjust to 7, 14, 60, 90, etc.)
    MAX_EVENTS_PER_PROCESSOR = 10000  # Limit to prevent very long queries

    # NiFi connection
    client = NiFiClient(
        "REPLACE_WITH_YOUR_NIFI_URL",           # e.g., "https://nifi.company.com:8443/nifi"
        username="REPLACE_WITH_YOUR_USERNAME",   # e.g., "admin"
        password="REPLACE_WITH_YOUR_PASSWORD",   # e.g., "password123"
        verify_ssl=False  # Set to True in production if you have valid SSL cert
    )

    # ========================================================================
    # PHASE 1: Calculate Date Range
    # ========================================================================
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_BACK)

    console.print(f"\n[yellow]Analyzing processor usage:[/yellow]")
    console.print(f"  Process Group: {TARGET_GROUP_ID[:16]}...")
    console.print(f"  Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    # ========================================================================
    # PHASE 2: Get Processors in Target Group
    # ========================================================================
    console.print(f"\n[yellow]Phase 1:[/yellow] Getting processors from target process group...")

    try:
        target_processors = client.list_processors(TARGET_GROUP_ID)
        console.print(f"[green]✓[/green] Found {len(target_processors)} processors")

        # Display processor list
        if target_processors:
            console.print("\n[cyan]Processors in target group:[/cyan]")
            for proc in target_processors[:10]:
                proc_name = proc['component']['name']
                proc_type = proc['component']['type'].split('.')[-1]
                console.print(f"  • {proc_name} ({proc_type})")
            if len(target_processors) > 10:
                console.print(f"  ... and {len(target_processors) - 10} more")

    except Exception as e:
        console.print(f"[red]✗[/red] Failed to get processors: {e}")
        client.close()
        return

    # ========================================================================
    # PHASE 3: Query Provenance Per Processor (with date range)
    # ========================================================================
    console.print(f"\n[yellow]Phase 2:[/yellow] Querying provenance (past {DAYS_BACK} days)...")

    processor_event_counts = {}

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task(
            f"Fetching events for {len(target_processors)} processors...",
            total=len(target_processors)
        )

        for proc in target_processors:
            processor_id = proc['id']
            proc_name = proc['component']['name']
            proc_type = proc['component']['type'].split('.')[-1]

            try:
                # Query with date range
                events = client.query_provenance(
                    processor_id=processor_id,
                    start_date=start_date,
                    end_date=end_date,
                    max_events=MAX_EVENTS_PER_PROCESSOR
                )

                processor_event_counts[proc_name] = {
                    'id': processor_id,
                    'count': len(events),
                    'type': proc_type
                }

                progress.advance(task)

            except Exception as e:
                console.print(f"[yellow]⚠[/yellow]  Failed for {proc_name}: {e}")
                processor_event_counts[proc_name] = {
                    'id': processor_id,
                    'count': 0,
                    'type': proc_type
                }
                progress.advance(task)

    console.print(f"[green]✓[/green] Found provenance for {len(processor_event_counts)} processors")

    # ========================================================================
    # PHASE 4: Generate Visualizations and Reports
    # ========================================================================
    console.print(f"\n[yellow]Phase 3:[/yellow] Generating reports...")

    # Sort by execution count (highest to lowest)
    sorted_processors = sorted(
        processor_event_counts.items(),
        key=lambda x: x[1]['count'],
        reverse=True
    )

    # 1. Save to CSV
    csv_file = Path(f"processor_usage_{TARGET_GROUP_ID[:8]}.csv")
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Processor Name', 'Processor Type', 'Event Count', 'Events per Day'])
        for name, data in sorted_processors:
            events_per_day = data['count'] / DAYS_BACK
            writer.writerow([name, data['type'], data['count'], f"{events_per_day:.1f}"])

    console.print(f"[green]✓[/green] Saved CSV: {csv_file}")

    # 2. Generate bar chart
    fig, ax = plt.subplots(figsize=(12, max(6, len(target_processors) * 0.3)))

    names = [name for name, _ in sorted_processors]
    counts = [data['count'] for _, data in sorted_processors]

    # Color bars: red for 0 events (unused), orange for low usage (<10), green for active
    colors = ['red' if c == 0 else 'orange' if c < 10 else 'green' for c in counts]

    ax.barh(names, counts, color=colors)
    ax.set_xlabel('Number of Provenance Events', fontsize=12)
    ax.set_ylabel('Processor Name', fontsize=12)
    ax.set_title(
        f'Processor Execution Frequency - Past {DAYS_BACK} Days\n'
        f'Process Group: {TARGET_GROUP_ID[:16]}...',
        fontsize=14,
        fontweight='bold'
    )
    ax.grid(axis='x', alpha=0.3, linestyle='--')

    plt.tight_layout()

    plot_file = Path(f"processor_usage_{TARGET_GROUP_ID[:8]}.png")
    plt.savefig(plot_file, dpi=150, bbox_inches='tight')
    console.print(f"[green]✓[/green] Saved plot: {plot_file}")

    # 3. Print summary
    total_events = sum(data['count'] for _, data in sorted_processors)
    unused_count = sum(1 for _, data in sorted_processors if data['count'] == 0)
    low_usage_count = sum(1 for _, data in sorted_processors if 0 < data['count'] < 10)

    console.print(f"\n[cyan]Summary:[/cyan]")
    console.print(f"  Total processors: {len(target_processors)}")
    console.print(f"  Total events: {total_events:,}")
    console.print(f"  Date range: {DAYS_BACK} days")
    console.print(f"  Unused processors (0 events): {unused_count}")
    console.print(f"  Low usage processors (<10 events): {low_usage_count}")

    if unused_count > 0:
        console.print(f"\n[yellow]⚠ Processors with 0 events (candidates for pruning):[/yellow]")
        for name, data in sorted_processors:
            if data['count'] == 0:
                console.print(f"  • {name} ({data['type']})")

    if low_usage_count > 0:
        console.print(f"\n[orange]⚠ Processors with low usage (<10 events):[/orange]")
        for name, data in sorted_processors:
            if 0 < data['count'] < 10:
                console.print(f"  • {name} ({data['type']}): {data['count']} events")

    console.print(f"\n[green]✓[/green] Analysis complete!")
    console.print(f"\n[cyan]Next steps:[/cyan]")
    console.print(f"  1. Review the bar chart: {plot_file}")
    console.print(f"  2. Review the CSV: {csv_file}")
    console.print(f"  3. Consider pruning unused processors")

    client.close()


if __name__ == "__main__":
    main()
