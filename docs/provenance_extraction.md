# Provenance Extraction

## Overview

The Provenance Extraction system is a core component of nifi2py that extracts real execution data from running NiFi flows. This data is used to:

1. **Inform Code Generation** - Understand what processors actually do with real data
2. **Validate Output** - Compare Python code output against actual NiFi output
3. **Detect Patterns** - Identify attribute transformations and content modifications

## Architecture

The provenance extraction system follows a **provenance-driven architecture**:

```
NiFi Provenance Repository
         ↓
  Query Provenance Events
         ↓
  Extract Samples (input/output)
         ↓
  Analyze Transformations
         ↓
  Generate Python Code
```

### Why Provenance-Driven?

Template parsing alone is insufficient because:
- Template IDs don't match live processor IDs
- Templates don't show actual runtime behavior
- Expression Language values are dynamic
- We need real I/O data for validation

Provenance provides:
- Actual input/output content
- Attribute transformations (before/after)
- Execution timestamps and patterns
- Real-world data samples

## Components

### 1. ExecutionSample

Represents a single execution of a processor captured from provenance:

```python
@dataclass
class ExecutionSample:
    event_id: int
    timestamp: datetime

    # Input
    input_content: Optional[bytes]
    input_attributes: Dict[str, str]

    # Output
    output_content: Optional[bytes]
    output_attributes: Dict[str, str]

    # Transformation
    attributes_added: Dict[str, str]
    attributes_modified: Dict[str, str]
    attributes_removed: List[str]
    content_changed: bool
```

**Key Features:**
- Captures complete before/after state
- Automatically calculates attribute diffs
- Tracks content modifications
- Links back to provenance event ID

### 2. ProcessorExecution

Aggregates multiple execution samples for a processor:

```python
@dataclass
class ProcessorExecution:
    processor_id: str
    processor_name: str
    processor_type: str

    executions: List[ExecutionSample]
    total_executions: int
    success_count: int
    failure_count: int
```

**Properties:**
- `has_samples: bool` - Whether any samples were collected
- `sample_coverage: float` - Percentage of executions captured

### 3. ProvenanceExtractor

Main class for extracting provenance data:

```python
extractor = ProvenanceExtractor(client)

# Extract for single processor
execution = extractor.extract_processor_executions(
    processor_id="abc-123",
    sample_size=10,
    start_time=datetime.now() - timedelta(hours=1),
    end_time=datetime.now()
)

# Extract for all processors in flow
all_executions = extractor.extract_flow_executions(
    group_id=None,  # None = root
    sample_size=10
)

# Analyze patterns
patterns = extractor.get_attribute_patterns(execution.executions)
content_summary = extractor.get_content_transformation_summary(execution.executions)
```

## Usage Examples

### Basic Extraction

```python
from nifi2py.client import NiFiClient
from nifi2py.provenance_extractor import ProvenanceExtractor

# Connect to NiFi
client = NiFiClient(
    "https://localhost:8443/nifi",
    username="admin",
    password="password",
    verify_ssl=False
)

# Create extractor
extractor = ProvenanceExtractor(client)

# Get processor ID (from template or API)
processor_id = "abc-123-def-456"

# Extract provenance samples
execution = extractor.extract_processor_executions(
    processor_id=processor_id,
    sample_size=5
)

# Analyze results
if execution.has_samples:
    print(f"Collected {len(execution.executions)} samples")

    # Show attribute changes
    for sample in execution.executions:
        print(f"\nEvent {sample.event_id}:")
        print(f"  Added: {sample.attributes_added}")
        print(f"  Modified: {sample.attributes_modified}")
        print(f"  Removed: {sample.attributes_removed}")
        print(f"  Content changed: {sample.content_changed}")
```

### Pattern Analysis

```python
# Get attribute transformation patterns
patterns = extractor.get_attribute_patterns(execution.executions)

for attr, counts in patterns.items():
    print(f"{attr}:")
    print(f"  Added: {counts['added']} times")
    print(f"  Modified: {counts['modified']} times")
    print(f"  Removed: {counts['removed']} times")

# Example output:
# timestamp:
#   Added: 10 times
#   Modified: 0 times
#   Removed: 0 times
# filename:
#   Added: 0 times
#   Modified: 5 times
#   Removed: 0 times
```

### Content Analysis

```python
# Get content transformation summary
summary = extractor.get_content_transformation_summary(execution.executions)

print(f"Total samples: {summary['total_samples']}")
print(f"Content changed: {summary['content_changed']}")
print(f"Content unchanged: {summary['content_unchanged']}")
print(f"Change percentage: {summary['change_percentage']:.1f}%")
```

### Flow-Wide Extraction

```python
# Extract for entire flow
all_executions = extractor.extract_flow_executions(sample_size=10)

# Analyze each processor
for proc_id, execution in all_executions.items():
    print(f"\n{execution.processor_name}:")
    print(f"  Type: {execution.processor_type}")
    print(f"  Samples: {len(execution.executions)}")

    if execution.executions:
        sample = execution.executions[0]
        print(f"  Attrs added: {len(sample.attributes_added)}")
        print(f"  Content changed: {sample.content_changed}")
```

## Integration with Code Generation

The provenance data directly informs code generation:

### UpdateAttribute Example

**Provenance shows:**
- Added attribute: `processed_date = "2026-01-03"`
- Added attribute: `processed_by = "nifi"`

**Generated code:**
```python
def process_add_metadata(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    flowfile.attributes['processed_date'] = datetime.now().strftime('%Y-%m-%d')
    flowfile.attributes['processed_by'] = 'nifi'
    return {'success': [flowfile]}
```

### RouteOnAttribute Example

**Provenance shows:**
- JSON files → `route.matched = "is_json"`
- CSV files → `route.matched = "is_csv"`

**Generated code:**
```python
def process_route_by_type(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    filename = flowfile.attributes.get('filename', '')

    if filename.endswith('.json'):
        return {'is_json': [flowfile]}
    elif filename.endswith('.csv'):
        return {'is_csv': [flowfile]}
    else:
        return {'unmatched': [flowfile]}
```

### ReplaceText Example

**Provenance shows:**
- Input: `b"Hello World"`
- Output: `b"Hello Python"`

**Generated code:**
```python
def process_replace_text(flowfile: FlowFile) -> Dict[str, List[FlowFile]]:
    content = flowfile.content.decode('utf-8')
    content = content.replace('World', 'Python')
    flowfile.content = content.encode('utf-8')
    return {'success': [flowfile]}
```

## Error Handling

The extractor handles various error conditions gracefully:

### No Provenance Data

```python
execution = extractor.extract_processor_executions(processor_id)

if not execution.has_samples:
    print("No provenance samples found")
    print("Possible reasons:")
    print("  - Processor hasn't executed yet")
    print("  - Provenance data has expired")
    print("  - Provenance repository is disabled")
```

### Permission Errors

```python
# If provenance queries are disabled (403 error)
execution = extractor.extract_processor_executions(processor_id)
# Returns ProcessorExecution with empty executions list
# Does not raise exception
```

### Missing Content

```python
# Content may not be available in provenance
sample = execution.executions[0]

if sample.input_content is None:
    print("Input content not available in provenance")

if sample.output_content is None:
    print("Output content not available in provenance")

# Can still analyze attribute transformations
print(f"Attributes added: {sample.attributes_added}")
```

## Command-Line Tool

Use the included analyzer script:

```bash
# Analyze first 5 processors, 5 samples each
python examples/analyze_provenance.py --processors 5 --samples 5

# Analyze all processors
python examples/analyze_provenance.py --all --samples 10

# Set NiFi connection via environment
export NIFI_URL=https://localhost:8443/nifi
export NIFI_USER=admin
export NIFI_PASS=password123
python examples/analyze_provenance.py
```

**Output:**
```
╭───────── Configuration ──────────╮
│ NiFi Provenance Analyzer         │
│ URL: https://localhost:8443/nifi │
│ Analyzing: 5 processors          │
│ Samples per processor: 5         │
╰──────────────────────────────────╯

Analyzing: UpdateAttribute_001
  ✓ Found 5 execution samples

  Attribute Transformations:
    • timestamp: added in 5 samples
    • filename: modified in 3 samples

  Content Transformations:
    • Changed: 0/5 (0%)
```

## Demo

Run the integration demo to see how provenance drives code generation:

```bash
python examples/demo_provenance_integration.py
```

This demonstrates:
- Mock provenance data from different processor types
- Attribute pattern analysis
- Content transformation detection
- Generated Python code based on patterns

## Best Practices

### 1. Sample Size

Choose appropriate sample sizes:
- **Small flows (< 10 processors):** 10-20 samples per processor
- **Medium flows (10-100 processors):** 5-10 samples per processor
- **Large flows (100+ processors):** 3-5 samples per processor

### 2. Time Windows

Use appropriate time windows:
```python
# Last hour (good for active flows)
start_time = datetime.now() - timedelta(hours=1)

# Last 24 hours (good for periodic flows)
start_time = datetime.now() - timedelta(days=1)

# Specific time range
start_time = datetime(2026, 1, 3, 9, 0, 0)
end_time = datetime(2026, 1, 3, 10, 0, 0)
```

### 3. Coverage Analysis

Check sample coverage:
```python
execution = extractor.extract_processor_executions(processor_id)

print(f"Sample coverage: {execution.sample_coverage:.1f}%")

if execution.sample_coverage < 50:
    print("Warning: Low sample coverage, may need more samples")
```

### 4. Content Size

Be aware of content size limitations:
- Provenance content may not be available for large FlowFiles
- Content repository may have size limits
- Consider using attribute analysis when content unavailable

## Troubleshooting

### No Samples Collected

**Problem:** `execution.executions` is empty

**Solutions:**
1. Check if processor has executed:
   ```python
   print(f"Total executions: {execution.total_executions}")
   ```

2. Verify provenance is enabled in NiFi
3. Check time window includes executions
4. Verify processor ID is correct

### Permission Denied

**Problem:** 403 error when querying provenance

**Solutions:**
1. Check user has provenance query permissions
2. Verify authentication credentials
3. Check NiFi authorization policies

### Missing Content

**Problem:** `input_content` and `output_content` are None

**Solutions:**
1. Check content claim is still available
2. Verify content repository retention
3. Fall back to attribute-only analysis

## Performance Considerations

### API Calls

Each extraction makes several API calls:
- 1 call to get processor info
- 1 call to query provenance
- 2N calls for content (where N = sample size)

For large flows, use batch extraction:
```python
# Extract all at once (more efficient)
all_executions = extractor.extract_flow_executions(sample_size=5)

# Instead of:
for proc_id in processor_ids:
    execution = extractor.extract_processor_executions(proc_id)
```

### Caching

Consider caching provenance results:
```python
import pickle

# Cache executions
with open('provenance_cache.pkl', 'wb') as f:
    pickle.dump(all_executions, f)

# Load cached executions
with open('provenance_cache.pkl', 'rb') as f:
    all_executions = pickle.load(f)
```

## Future Enhancements

Planned improvements:
1. **Async extraction** - Parallel provenance queries
2. **Pattern learning** - ML-based pattern detection
3. **Diff visualization** - Visual diff of before/after
4. **Validation integration** - Direct validation from provenance
5. **Provenance replay** - Re-execute flows from provenance
