# Getting Started with nifi2py

Quick guide to convert your NiFi flows to Python and validate the results.

---

## Prerequisites

1. **NiFi instance** running with provenance enabled
2. **Recent flow executions** (to have provenance data)
3. **Python 3.8+** with nifi2py installed

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies (if needed)
pip install -e .
```

---

## 5-Minute Quick Start

### Step 1: Configure Connection

Edit `examples/provenance_to_python.py` with your NiFi details:

```python
client = NiFiClient(
    "https://your-nifi-host:8443/nifi",  # ← Update this
    username="your-username",             # ← Update this
    password="your-password",             # ← Update this
    verify_ssl=False
)
```

### Step 2: Generate Python Code

```bash
python examples/provenance_to_python.py
```

**Output:** `generated/generated_from_provenance.py`

### Step 3: Validate

```bash
python examples/validate_generated_code.py
```

**You'll see:**
- ✅ Processor coverage (should be ~100%)
- ✅ Path coverage (shows execution paths)
- ✅ Structure validation results
- ⚠️ Content validation (if provenance content available)

### Step 4: Test

```bash
python examples/run_generated_flow.py
```

**You'll see:**
- Test execution of generated code
- FlowFile transformations
- Attribute changes
- Content previews

---

## Understanding Validation Results

### Good Results ✅
```
Structure Validation:
  Substantive processors in flow: 5
  Processors generated: 5
  Processor coverage: 100.0%

  Execution paths found: 5
  Execution paths implemented: 5
  Path coverage: 100.0%

Content Validation:
  Content unavailable (normal if content repository not configured)
```

### What This Means
- **100% processor coverage** = All data-transforming processors implemented
- **100% path coverage** = All execution paths covered
- **Content unavailable** = Normal if NiFi's content repository isn't configured or content expired

---

## Common Scenarios

### Scenario 1: Content Available (Ideal)
```
Content Validation Results:
  Total events: 10
  Testable: 10
  Matched: 10
  Mismatched: 0
  Actual parity: 100%
```

**Meaning:** Perfect! Generated code produces identical output to NiFi.

---

### Scenario 2: Content Not Available (Common)
```
Content Validation Results:
  Total events: 10
  Testable: 0
  Content unavailable: 10
```

**Meaning:** This is OK! Use structure validation + external system validation.

**Next steps:**
1. Structure validation passed ✓
2. Test with sample data: `python examples/run_generated_flow.py`
3. Validate against external system (if applicable)

---

### Scenario 3: Data Goes to External System
```bash
# Your NiFi flow sends data to:
# - PostgreSQL (PutSQL)
# - HDFS (PutFile)
# - REST API (InvokeHTTP)
```

**Solution:** See `examples/validate_external_system.py` for:
- Database validation
- File validation
- API validation

---

## Troubleshooting

### "No provenance events found"
**Cause:** Flow hasn't been executed recently

**Solution:**
1. Run your NiFi flow
2. Wait for data to process
3. Try code generation again

---

### "Connection refused"
**Cause:** Wrong NiFi URL, credentials, or NiFi not running

**Solution:**
1. Verify NiFi is running: `https://your-host:8443/nifi`
2. Check username/password
3. Update `examples/provenance_to_python.py`

---

### "Processor coverage: 60%"
**Cause:** Some processors not yet supported

**Solution:**
1. Check `PROJECT_STATUS.md` for supported processors
2. Unsupported processors won't affect validation if they're no-ops (Funnel, LogMessage)
3. For production use, may need to wait for Phase 2 (UpdateAttribute, RouteOnAttribute, etc.)

---

### "Content unavailable"
**Cause:** Normal - NiFi's provenance content repository not configured or content expired

**Solution:**
1. ✅ Structure validation still works
2. ✅ Test with sample data
3. ✅ Validate against external systems
4. ⚪ To enable content validation:
   - Configure NiFi's `nifi.provenance.repository.max.storage.time`
   - Re-run flow
   - Content will be available for recent executions

---

## What's Supported? (Phase 1)

### ✅ Supported Processors
- **DetectDuplicate** - Deduplication with cache
- **ExtractText** - Regex-based attribute extraction
- **RouteText** - Pattern-based routing
- **SplitText** - Line-based splitting
- **GetHTTP** - HTTP content fetching

### ⚪ Coming in Phase 2
- **UpdateAttribute** - Attribute manipulation (high priority)
- **RouteOnAttribute** - Conditional routing
- **ExecuteStreamCommand** - Shell commands → Databricks
- **Additional processors** - See `PROJECT_STATUS.md`

---

## Next Steps

### For Development
1. Review `PROJECT_STATUS.md` for roadmap
2. Check `docs/` for technical documentation
3. Explore `examples/` for more use cases

### For Production Use
1. Test with your NiFi flows
2. Validate results thoroughly
3. Check processor support in `PROJECT_STATUS.md`
4. Plan for Phase 2 if needed

### For Contribution
1. Review `CLAUDE.md` for architecture
2. Check `docs/el_transpiler_design.md` for EL details
3. See `nifi2py/processor_converters.py` for examples

---

## Key Files

| File | Purpose |
|------|---------|
| `README.md` | Project overview |
| `PROJECT_STATUS.md` | **Phased development plan** |
| `VALIDATION_GUIDE.md` | Complete validation documentation |
| `examples/README.md` | Examples documentation |
| `docs/README.md` | Technical docs index |

---

## Quick Reference

```bash
# Generate code
python examples/provenance_to_python.py

# Validate
python examples/validate_generated_code.py

# Test
python examples/run_generated_flow.py

# Check project status
cat PROJECT_STATUS.md
```

---

## Questions?

- **Architecture:** See `CLAUDE.md`
- **Validation:** See `VALIDATION_GUIDE.md`
- **Status/Roadmap:** See `PROJECT_STATUS.md`
- **Examples:** See `examples/README.md`
- **Technical Docs:** See `docs/README.md`

---

**Ready to convert your NiFi flows!** 🚀
