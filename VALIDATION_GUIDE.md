# Validation Guide

## Two-Tier Validation Approach

nifi2py supports **two types of validation** to handle real-world scenarios:

### 1. Structure Validation (Always Available)
**Works WITHOUT content** - uses only provenance metadata

Validates:
- ✅ Flow graph structure matches
- ✅ Processor sequences are correct
- ✅ Execution paths are implemented
- ✅ Relationship routing is present

**Excludes no-op processors** from coverage:
- Funnel (just routes flowfiles)
- LogMessage, LogAttribute (monitoring only)
- ControlRate (throttling only)
- MonitorActivity (monitoring only)

**When to use**: Always run this first - it works even when content is not available.

### 2. Content Validation (When Available)
**Requires provenance content** - compares actual byte-for-byte output

Validates:
- ✅ Python output matches NiFi output exactly
- ✅ Attributes are set correctly
- ✅ Content transformations are correct

**When to use**: When provenance content repository is configured and content is fresh.

---

## Why Exclude No-Op Processors?

Some NiFi processors don't transform data - they're for monitoring, routing, or coordination:

| Processor | Purpose | Why Excluded |
|-----------|---------|--------------|
| **Funnel** | Combines connections | Just a routing point, no logic |
| **LogMessage** | Logs messages | Debugging only, doesn't change data |
| **LogAttribute** | Logs attributes | Debugging only, doesn't change data |
| **ControlRate** | Throttles throughput | Timing control, no transformation |
| **MonitorActivity** | Monitors flow | Monitoring only, no transformation |

**In Python**: These processors either pass data through unchanged or are omitted entirely. They don't affect validation coverage because there's no transformation logic to validate.

---

## Validation Scenarios

### Scenario 1: Content Available (Ideal)
```bash
# Run comprehensive validation
python examples/validate_generated_code.py
```

**Result**:
- Structure validation: ✓ Pass
- Content validation: ✓ 100% parity
- **Conclusion**: Generated code is correct!

### Scenario 2: Content Not Available (Common)
```bash
# Run structure validation only
python examples/validate_generated_code.py
```

**Result**:
- Structure validation: ✓ Pass
- Content validation: ⚠ Content unavailable
- **Conclusion**: Structure is correct, validate against external system

### Scenario 3: Data Goes to External System
```bash
# 1. Run structure validation
python examples/validate_generated_code.py

# 2. Run external system validation
python examples/validate_external_system.py
```

**Or implement custom validation**:
```python
from nifi2py.graph_validator import validate_external_output

# Get data from external system (database, file, API)
db_data = fetch_from_database("SELECT * FROM nifi_output")

# Run generated Python code
python_output = execute_path_1(input_data)

# Compare
matched, mismatched, total = validate_external_output(
    db_data, python_output, comparison_key='id'
)

print(f"Validation: {matched}/{total} matched ({matched/total*100:.1f}%)")
```

---

## External System Validation Examples

### Example 1: Database (SQL)
**Scenario**: NiFi uses PutSQL to write to PostgreSQL

```python
import psycopg2
from generated.generated_from_provenance import *

# 1. Get NiFi output from database
conn = psycopg2.connect("dbname=mydb user=postgres")
cur = conn.execute("SELECT * FROM nifi_output WHERE timestamp > NOW() - INTERVAL '1 hour'")
db_results = cur.fetchall()

# 2. Run generated Python
python_results = execute_path_1(input_data)

# 3. Compare
# ... comparison logic ...
```

### Example 2: Files (HDFS/Local)
**Scenario**: NiFi uses PutFile to write files

```bash
# 1. Compare file counts
nifi_count=$(ls /nifi/output/*.txt | wc -l)
python_count=$(ls /python/output/*.txt | wc -l)

# 2. Compare file contents
for file in /nifi/output/*.txt; do
    basename=$(basename $file)
    diff $file /python/output/$basename
done
```

### Example 3: REST API
**Scenario**: NiFi uses InvokeHTTP to POST data

```python
import requests

# 1. Get API logs for NiFi calls
nifi_calls = requests.get("https://api.example.com/logs?source=nifi").json()

# 2. Run generated Python and capture API calls
# (instrument your code to log API calls)
python_calls = run_and_capture_api_calls(execute_path_1, input_data)

# 3. Compare
for nifi_call, python_call in zip(nifi_calls, python_calls):
    assert nifi_call['url'] == python_call['url']
    assert nifi_call['body'] == python_call['body']
```

---

## Validation Decision Tree

```
Start
  ↓
Is provenance content available?
  ├─ Yes → Run comprehensive validation
  │         ├─ Structure: ✓
  │         └─ Content: ✓ or ✗
  │            └─ If ✓: Done! Code is validated
  │            └─ If ✗: Fix generated code
  │
  └─ No → Run structure validation only
            ├─ Structure: ✓ or ✗
            │  └─ If ✗: Regenerate code
            │  └─ If ✓: Continue ↓
            │
            └─ Does data go to external system?
               ├─ Yes → Validate against external system
               │         ├─ Database: Compare SQL results
               │         ├─ Files: Compare file contents
               │         └─ API: Compare API calls
               │
               └─ No → Test with sample data
                        └─ python examples/run_generated_flow.py
```

---

## Configuration Indicators

### Indicate External Validation Needed

When generating code, we can indicate where external validation is needed:

```python
def execute_path_3(initial_flowfile=None):
    """
    Execute Path 3: ExtractText → Transform → PutSQL

    EXTERNAL VALIDATION REQUIRED:
      System: PostgreSQL Database
      Table: output_table
      Validation: Compare results with:
        SELECT * FROM output_table WHERE created_at > NOW() - INTERVAL '1 hour'
    """
    # ... generated code ...
```

### Auto-Detection

The code generator can detect egress processors and add validation hints:

```python
# Detected egress processors:
EGRESS_PROCESSORS = {
    'PutSQL': 'Validate against database',
    'PutFile': 'Validate against filesystem',
    'PutHDFS': 'Validate against HDFS',
    'InvokeHTTP': 'Validate against API logs',
    'PublishKafka': 'Validate against Kafka topic',
    'PutEmail': 'Validate email logs',
}
```

---

## Best Practices

### 1. Always Start with Structure Validation
```bash
python examples/validate_generated_code.py
```
This works 100% of the time and validates the flow graph.

### 2. Use Content Validation When Available
If you have fresh provenance data with content, use it:
- Proves Python output = NiFi output
- Fastest validation method
- No external system setup needed

### 3. Plan for External Validation
For production systems:
- Document where NiFi sends data
- Set up validation queries/scripts
- Run validation as part of testing

### 4. Combine Multiple Methods
```bash
# 1. Structure validation (always)
python examples/validate_generated_code.py

# 2. Content validation (if available)
# Already run in step 1

# 3. External system validation (for critical paths)
python examples/validate_external_system.py

# 4. Manual testing with known data
python examples/run_generated_flow.py
```

---

## Validation Coverage Goals

| Validation Type | Target Coverage | Notes |
|----------------|-----------------|-------|
| Structure | 100% | Should always pass |
| Content | Best effort | Depends on content availability |
| External | Critical paths | Focus on production outputs |
| Manual testing | Sample data | Verify end-to-end |

---

## Troubleshooting

### "Content not available"
**Cause**: Provenance content expired or not configured

**Solution**:
1. Check NiFi provenance settings: `nifi.provenance.repository.max.storage.time`
2. Run your flow again to generate fresh provenance
3. Use external validation instead

### "Low structure coverage"
**Cause**: Processors deleted or configs unavailable

**Solution**:
1. Regenerate code from fresh provenance
2. Check for deleted processors in NiFi
3. Verify REST API access

### "Mismatched content"
**Cause**: Generated code logic differs from NiFi

**Solution**:
1. Review processor configurations
2. Check EL expression transpilation
3. Verify relationship routing
4. File a bug report with details

---

## Summary

✅ **Use structure validation** - Always works, validates flow graph

✅ **Use content validation** - When available, proves correctness

✅ **Use external validation** - For production systems, validates end-to-end

✅ **Combine all three** - Maximum confidence in generated code!
