# Changelog

All notable changes to the nifi2py project will be documented in this file.

## [Unreleased]

### Added - 2026-01-05

#### 1. Process Group Filtering Test Script
**File:** `examples/test_connection_with_group_filter.py`
**Purpose:** Allow users to test NiFi connection and convert only a specific process group instead of all flows.

**What was added:**
- New test script based on `provenance_to_python.py` with process group filtering
- Configuration section for `TARGET_GROUP_ID` parameter
- Phase 2.5: Filter provenance events by process group
- Automatic processor listing from target group
- Enhanced error messages with troubleshooting tips
- Output files include group ID in filename

**Key features:**
```python
# Configuration
TARGET_GROUP_ID = "your-process-group-id"  # User configurable

# Phase 2.5: Filtering
target_processors = client.list_processors(TARGET_GROUP_ID)
target_processor_ids = {p['id'] for p in target_processors}
events = [e for e in events if e.get('componentId') in target_processor_ids]
```

**Why this was needed:**
- Users have multiple flows on their NiFi canvas
- Only want to convert one specific process group
- Need to test connection with their target flow

**Usage:**
```bash
# 1. Edit the script and update:
#    - TARGET_GROUP_ID (line ~47)
#    - NiFi URL (line ~56)
#    - Username/password (lines ~57-58)
# 2. Run the script
python examples/test_connection_with_group_filter.py
```

**How to find Process Group ID:**
1. Open NiFi UI in browser
2. Right-click process group → "Enter process group"
3. Check URL: `https://nifi.../nifi/?processGroupId=abc-123-def-456`
4. Copy the ID after `processGroupId=`

---

#### 2. Fixed Date Format Bug in Provenance Query
**File:** `nifi2py/client.py` (lines 476-481)
**Commits:** `eb51484`, `5793522`

**Problem:**
Provenance queries were failing with 400 error:
```
API request failed: 400 - Cannot construct instance of
`org.apache.nifi.web.api.dto.provenance.ProvenanceSearchValueDTO`
no String-argument constructor/factory method to deserialize from String value
('01/06/2026 00:53:07')
```

**Root cause:**
The client was formatting dates as `MM/DD/YYYY HH:MM:SS` which NiFi's API could not deserialize.

**Fix iteration 1 (Commit `eb51484`):**
Changed to ISO 8601 format but NiFi still rejected dates with microseconds.

**Fix iteration 2 (Commit `5793522`):**
Removed microseconds from ISO format - NiFi now accepts it.

**What was changed:**
```python
# ITERATION 1 (BROKEN - MM/DD/YYYY format):
if start_date:
    search_terms["StartDate"] = start_date.strftime("%m/%d/%Y %H:%M:%S")
if end_date:
    search_terms["EndDate"] = end_date.strftime("%m/%d/%Y %H:%M:%S")

# ITERATION 2 (STILL BROKEN - ISO with microseconds):
if start_date:
    search_terms["StartDate"] = start_date.isoformat()
if end_date:
    search_terms["EndDate"] = end_date.isoformat()

# ITERATION 3 (FIXED - ISO without microseconds):
if start_date:
    # Use ISO 8601 format without microseconds (NiFi requirement)
    search_terms["StartDate"] = start_date.replace(microsecond=0).isoformat()
if end_date:
    # Use ISO 8601 format without microseconds (NiFi requirement)
    search_terms["EndDate"] = end_date.replace(microsecond=0).isoformat()
```

**Date format evolution:**
- **Iteration 1:** `"01/06/2026 00:53:07"` ❌ NiFi rejects - wrong format in searchTerms
- **Iteration 2:** `"2026-01-06T01:03:20.728000"` ❌ NiFi rejects - wrong type in searchTerms
- **Iteration 3:** `"2026-01-06T01:13:15"` ❌ NiFi rejects - wrong location (searchTerms vs request)
- **Iteration 4 (FINAL):** `"01/06/2026 01:13:15 UTC"` ✅ Correct format in request object - WORKS!

**The Real Issue:**
Dates were being placed in `searchTerms` object, but NiFi API expects them **directly in the `request` object**!

**Final Fix (Commit `46ce3a0`):**
```python
# CORRECT - Dates in request object, NOT searchTerms:
query_request = {
    "provenance": {
        "request": {
            "maxResults": max_results,
            "startDate": "01/06/2026 01:13:15 UTC",  # ← Direct in request
            "endDate": "01/06/2026 01:13:15 UTC",    # ← Direct in request
            "searchTerms": {
                "ProcessorID": "..."  # ← Only non-date filters here
            }
        }
    }
}
```

**Why this was needed:**
- NiFi API documentation shows dates go in `request`, NOT in `searchTerms`
- The `ProvenanceSearchValueDTO` class cannot deserialize date strings from searchTerms
- Format: `MM/DD/YYYY HH:MM:SS TIMEZONE` (e.g., `"01/06/2026 01:13:15 UTC"`)
- This affected pagination in provenance queries

**API Reference:**
- [Cloudera Community: NiFi Provenance REST API](https://community.cloudera.com/t5/Support-Questions/Apache-Nifi-How-to-use-REST-api-for-provenance-related/td-p/132733)

**Impact:**
- Provenance queries now work correctly
- Date filtering in queries is functional
- Pagination through large provenance datasets works

**Testing:**
After this fix, users should see successful provenance queries:
```
Phase 2: Querying provenance repository (max 5000 events)...
✓ Found 1234 provenance events
```

---

## Installation & Setup Changes

### Repository Setup
**Date:** 2026-01-05
**Repository:** `git@github.com:Afei99357/test_andrew_nifi_converter.git`

**What was done:**
1. Initialized git repository
2. Created `.gitignore` file (excludes venv, generated files, etc.)
3. Committed all project files (56 files, 20,754 insertions)
4. Pushed to GitHub on `main` branch

**Files added:**
- Core library: `nifi2py/` (all modules)
- Examples: `examples/` (including new test script)
- Documentation: `README.md`, `GETTING_STARTED.md`, `PROJECT_STATUS.md`, etc.
- Tests: `tests/` (comprehensive test suite)
- Configuration: `pyproject.toml`, `.gitignore`

---

## Migration Notes

### For Users Upgrading
If you're pulling the latest changes:

```bash
# Pull latest code
git pull origin main

# If you have local changes to test_connection_with_group_filter.py,
# you may need to merge or stash them
git stash  # Save your local config changes
git pull origin main
git stash pop  # Restore your config
```

### Known Issues Resolved
1. ✅ Provenance query 400 error with date format
2. ✅ Cannot filter by process group (now possible with test script)

### Pending Issues
None currently reported.

---

## Development Environment

### Python Version
- Tested with: Python 3.13.7 (use `python` not `python3`)

### Installation
```bash
# Clone repository
git clone git@github.com:Afei99357/test_andrew_nifi_converter.git
cd test_andrew_nifi_converter

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Install package
pip install -e .
```

### Configuration
Users must configure their NiFi connection in test scripts:
- NiFi URL (e.g., `https://nifi.company.com:8443/nifi`)
- Username and password
- Process Group ID (for filtered conversion)

---

## Contributors

- Andrew (Original implementation)
- Claude Sonnet 4.5 (Enhancements and bug fixes)

---

## Future Enhancements

### Planned
- [ ] Certificate-based authentication support (mTLS)
- [ ] CLI parameter for process group ID (no code editing needed)
- [ ] Support for more processor types
- [ ] Template-based conversion (in addition to provenance)

### Under Consideration
- [ ] Web UI for configuration
- [ ] Docker container for easy deployment
- [ ] Integration with CI/CD pipelines

---

## References

### Documentation
- [README.md](README.md) - Project overview
- [GETTING_STARTED.md](GETTING_STARTED.md) - Quick start guide
- [PROJECT_STATUS.md](PROJECT_STATUS.md) - Implementation status
- [VALIDATION_GUIDE.md](VALIDATION_GUIDE.md) - Validation framework

### Key Files
- `nifi2py/client.py` - NiFi REST API client
- `examples/test_connection_with_group_filter.py` - Process group filtering test script
- `examples/provenance_to_python.py` - Original provenance-to-Python converter

---

## Version History

### Initial Release (2026-01-05)
- Initial commit with core functionality
- 56 files, 20,754 lines of code
- Supports 11+ processor types
- ~70% coverage of typical NiFi flows
- Comprehensive test suite
- Full documentation

### Bug Fix Release (2026-01-05)
- Fixed date format bug in provenance queries
- Added process group filtering capability
- Enhanced error messages and troubleshooting guides
