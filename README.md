# nifi2py

Convert Apache NiFi dataflows to Python code using provenance-driven analysis.

> **📋 Project Status:** See [PROJECT_STATUS.md](PROJECT_STATUS.md) for implementation phases, supported processors, and roadmap.

## 🎯 What It Does

nifi2py generates **functional Python code** from NiFi flows by analyzing actual execution data (provenance), not just templates. The generated code includes:

- ✅ Functional processor implementations (not stubs)
- ✅ Correct execution sequences from FlowFile lineage
- ✅ Ready-to-run workflow functions

## 🚀 Quick Start

```bash
# Activate virtual environment
source venv/bin/activate

# Generate Python code from your NiFi flow
python examples/provenance_to_python.py

# Test the generated code
python examples/run_generated_flow.py
```

## 📁 Project Structure

```
nifi2py/
├── nifi2py/                    # Core library
│   ├── client.py              # NiFi REST API client (with pagination)
│   ├── lineage_tracer.py      # FlowFile lineage analysis
│   ├── processor_converters.py # Processor-specific code generators
│   └── ...
├── examples/                   # Example scripts
│   ├── provenance_to_python.py    # Main code generator
│   ├── run_generated_flow.py      # Test generated code
│   ├── test_lineage_tracer.py     # Test lineage tracing
│   └── walk_provenance.py         # Walk through provenance events
├── tests/                      # Unit tests
├── generated/                  # Generated code output
│   ├── generated_from_provenance.py  # Generated functions + workflows
│   └── provenance_analysis.json      # Provenance metadata
├── docs/                       # Documentation
├── CLAUDE.md                   # Architecture for Claude Code
├── README.md                   # This file
└── pyproject.toml             # Project configuration
```

## 💡 How It Works

### Provenance-Driven Approach

```
NiFi Flow Execution
    ↓
Provenance Events (what actually happened)
    ↓
FlowFile Lineage (parent/child relationships)
    ↓
Processor Configs (via REST API)
    ↓
Generated Python Code (functional + workflow execution)
```

**Why NOT template-driven?**
- Templates don't have runtime data
- Template IDs don't match live processor IDs
- No way to validate generated code

**Why provenance-driven?**
- ✅ Based on actual execution
- ✅ Has input/output data for validation
- ✅ Captures real FlowFile journeys
- ✅ Works with any flow structure

## 🔧 Usage

### 1. Generate Code from NiFi Flow

```bash
# Make sure your NiFi flow has been running and has provenance data
python examples/provenance_to_python.py
```

**Output**:
```
Phase 1: Connecting to NiFi...
✓ Connected successfully

Phase 2: Querying provenance repository (max 5000 events)...
✓ Found 492 provenance events

Phase 3: Grouping events by processor...
✓ Found 7 unique processors

Phase 4: Fetching processor configurations...
✓ Fetched 5 processor configs

Phase 5: Generating Python code from provenance...
✓ Generated functional code for each processor

Phase 5.5: Analyzing FlowFile lineage...
✓ Found 5 execution paths
  • Path 1: ExtractText → DetectDuplicate
  • Path 2: RouteText → SplitText
  • Path 3: ExtractText → DetectDuplicate → ...
  • Path 4: RouteText
  • Path 5: GetHTTP → Funnel → RouteText

Phase 6: Saving results...
✓ Generated Python code: generated/generated_from_provenance.py
✓ Provenance analysis: generated/provenance_analysis.json
```

### 2. Test Generated Code

```bash
python examples/run_generated_flow.py
```

**Output**:
```
Testing Path 5: GetHTTP → RouteText
✓ Fetched 48,796 bytes from http://www.apache.org/
✓ Routed to 'potential URL' relationship
✓ Execution completed successfully
```

### 3. Use Generated Code in Your Project

```python
from generated.generated_from_provenance import *

# Execute complete workflow
flowfiles = execute_path_5()  # GetHTTP → RouteText
print(f"Fetched {len(flowfiles[0].content)} bytes")

# Execute specific processor
test_ff = FlowFile(content=b"Check out https://apache.org/")
result = process_extracttext_6b49d3c2_643d_43(test_ff)
print(f"Extracted: {result['success'][0].attributes['url']}")

# Execute extraction + deduplication path
flowfiles = execute_path_1(test_ff)  # ExtractText → DetectDuplicate
```

## 🎨 Supported Processors

Currently supports functional code generation for:

| Processor | Status | Features |
|-----------|--------|----------|
| DetectDuplicate | ✅ | Cache-based duplicate detection using `${cache_identifier}` |
| ExtractText | ✅ | Regex pattern extraction from configuration |
| RouteText | ✅ | Pattern-based routing with ignore case support |
| SplitText | ✅ | Content splitting with fragment tracking |
| GetHTTP | ✅ | HTTP fetching with timeout and error handling |
| UpdateAttribute | ⏳ | Planned |
| InvokeHTTP | ⏳ | Planned |
| RouteOnAttribute | ⏳ | Planned |

## 🔍 Key Features

### 1. FlowFile Lineage Tracing

Automatically discovers execution paths from provenance:

```python
from nifi2py.lineage_tracer import LineageTracer

tracer = LineageTracer(provenance_events)

# Get all execution paths discovered from actual flow execution
paths = tracer.get_execution_paths()
for i, path in enumerate(paths):
    print(f"Path {i+1}:")
    for proc_id, proc_type, proc_name in path:
        print(f"  → {proc_type}: {proc_name}")

# Trace specific FlowFile through its complete journey
lineage = tracer.trace_lineage(flowfile_uuid)
tracer.print_lineage(flowfile_uuid)

# Find ingress/egress points
roots = tracer.get_root_flowfiles()     # Where FlowFiles enter
leaves = tracer.get_leaf_flowfiles()    # Where FlowFiles exit or are dropped
```

### 2. Expression Language Transpilation

Converts NiFi EL to Python:

| NiFi EL | Python |
|---------|--------|
| `${url}` | `flowfile.attributes.get('url', '')` |
| `${filename:toUpper()}` | `flowfile.attributes.get('filename', '').upper()` |
| `${filename:substringBefore('.')}` | `flowfile.attributes.get('filename', '').split('.')[0]` |
| `${now()}` | `datetime.now().isoformat()` |

### 3. Provenance Pagination

Handles large flows with automatic pagination:

```python
# Automatically pages through provenance to collect all events
events = client.query_provenance(max_events=10000)

# Behind the scenes:
# - Fetches in chunks of 1000
# - Uses oldest event timestamp for next page
# - Continues until max_events reached
```

### 4. Validation Framework

Compare generated Python output with NiFi's actual output:

```python
# Coming soon - see "Validation" section below
validator = ProvenanceValidator(client, generated_module)
results = validator.validate(sample_size=100)
print(f"Parity: {results.parity_percentage}%")
```

## 📚 Examples

### Web Crawler Example

Your web crawler flow generates this execution sequence:

```
GetHTTP: Get from seed URL
  ↓
Funnel
  ↓
RouteText: Find values of interest
  ↓ (potential URL)
SplitText: Split by lines
  ↓
ExtractText: Extract URL
  ↓
DetectDuplicate: Remove duplicates
```

Generated code:

```python
def execute_path_5(initial_flowfile=None):
    """Complete web crawler workflow"""

    # Fetch seed URL
    result = process_gethttp_xxx(initial_flowfile or FlowFile())
    flowfiles = result['success']

    # Route content
    result = process_routetext_xxx(flowfiles[0])
    flowfiles = result['potential URL']

    # Split and extract URLs
    # ... continues with correct sequence
```

## 🧪 Testing

```bash
# Test lineage tracer
python examples/test_lineage_tracer.py

# Test generated code
python examples/test_generated_code.py

# Run unit tests
pytest tests/
```

## 🔧 Configuration

Create a config file or modify scripts directly:

```python
client = NiFiClient(
    "https://your-nifi-host:8443/nifi",
    username="your-username",
    password="your-password",
    verify_ssl=False  # Set True in production
)
```

## 🎯 Use Cases

- **Migration**: Convert NiFi flows to Python for Databricks, Airflow, AWS Lambda
- **Testing**: Validate NiFi logic in unit tests
- **Documentation**: Generate readable Python from complex flows
- **Learning**: Understand NiFi flows through Python code
- **Optimization**: Profile Python version vs NiFi

## 📖 Documentation

- [CLAUDE.md](CLAUDE.md) - Architecture and design decisions for Claude Code
- [QUICK_START.md](QUICK_START.md) - Detailed getting started guide
- [archive/reports/](archive/reports/) - Implementation reports and summaries
