# Examples

This directory contains working examples demonstrating nifi2py's key capabilities.

## Quick Start

### 1. Generate Python Code from NiFi Provenance

```bash
python examples/provenance_to_python.py
```

**What it does:**
- Connects to your NiFi instance
- Fetches provenance events from recent flow executions
- Analyzes FlowFile lineage to understand execution paths
- Generates executable Python code that replicates the NiFi flow

**Output:** `generated/generated_from_provenance.py`

---

### 2. Validate Generated Code

```bash
python examples/validate_generated_code.py
```

**What it does:**
- **Structure Validation** - Verifies flow graph structure matches (always works, no content needed)
  - Processor coverage
  - Execution path coverage
  - Relationship routing
- **Content Validation** - Compares Python output vs NiFi output byte-for-byte (when provenance content available)

**Use this to:** Ensure generated code is correct and complete.

---

### 3. Run Generated Code

```bash
python examples/run_generated_flow.py
```

**What it does:**
- Executes the generated Python code with test data
- Shows FlowFile transformations
- Displays attributes and content changes
- Tests multiple execution paths

**Use this to:** Test generated code with sample inputs.

---

### 4. Validate Against NiFi (Content-Based)

```bash
python examples/validate_against_nifi.py
```

**What it does:**
- Fetches actual NiFi output from provenance events
- Runs same input through generated Python code
- Compares outputs using SHA256 hashes
- Reports parity percentage

**Use this when:** Provenance content repository is configured and you want to verify byte-for-byte accuracy.

**Note:** Requires provenance content to be available (not expired).

---

### 5. Validate Against External Systems

```bash
python examples/validate_external_system.py
```

**What it does:**
- Shows examples of validating when NiFi sends data to external systems
- Database validation (PutSQL → PostgreSQL)
- File validation (PutFile → HDFS/local)
- API validation (InvokeHTTP → REST API)

**Use this when:**
- Provenance content is not available
- NiFi writes to databases, files, or APIs
- You want end-to-end validation

---

## Example Workflow

**Typical workflow for converting a NiFi flow:**

```bash
# 1. Generate Python code
python examples/provenance_to_python.py

# 2. Validate structure and content
python examples/validate_generated_code.py

# 3. Test with sample data
python examples/run_generated_flow.py

# 4. (Optional) Validate against external systems if needed
python examples/validate_external_system.py
```

---

## Configuration

All examples connect to NiFi using these defaults (edit scripts to customize):

```python
NiFiClient(
    "https://127.0.0.1:8443/nifi",
    username="apsaltis",
    password="deltalakeforthewin",
    verify_ssl=False
)
```

**Update these values** in each script to match your NiFi instance.

---

## Requirements

These examples require:
- ✅ NiFi instance running with provenance enabled
- ✅ Recent flow executions (to have provenance data)
- ✅ Python environment with nifi2py installed

Optional:
- ⚪ Provenance content repository configured (for content validation)
- ⚪ External systems configured (for external validation)

---

## Troubleshooting

### "No provenance events found"
**Solution:** Run your NiFi flow to generate provenance data, then try again.

### "Content not available"
**Solution:** This is normal. Use structure validation + external system validation instead.

### "Connection refused"
**Solution:** Check NiFi URL, username, and password in the example script.

---

## See Also

- [Validation Guide](../VALIDATION_GUIDE.md) - Complete validation documentation
- [README](../README.md) - Project overview and architecture
- [Generated Code](../generated/) - Output directory for generated Python files
