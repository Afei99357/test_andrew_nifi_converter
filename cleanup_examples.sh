#!/bin/bash
# Clean up examples directory for shareable project
# Keep only essential, polished examples

set -e

echo "============================================"
echo "Cleaning up examples directory..."
echo "============================================"

# Create archive structure
mkdir -p archive/old_examples
mkdir -p archive/old_docs
mkdir -p test-data

# ============================================================================
# Keep these core examples (the polished, new ones)
# ============================================================================
echo "✓ Keeping core examples:"
echo "  - validate_generated_code.py (comprehensive validation)"
echo "  - validate_against_nifi.py (content validation)"
echo "  - validate_external_system.py (external system validation)"
echo "  - run_generated_flow.py (test runner)"
echo "  - provenance_to_python.py (main generation script)"

# ============================================================================
# Move old/development scripts to archive
# ============================================================================
echo ""
echo "Moving old development scripts to archive/old_examples/..."

# Test scripts
mv examples/test_*.py archive/old_examples/ 2>/dev/null || true

# Analysis scripts
mv examples/analyze_*.py archive/old_examples/ 2>/dev/null || true

# Demo/debug scripts
mv examples/client_demo.py archive/old_examples/ 2>/dev/null || true
mv examples/models_demo.py archive/old_examples/ 2>/dev/null || true
mv examples/quick_reference.py archive/old_examples/ 2>/dev/null || true
mv examples/demo_provenance_integration.py archive/old_examples/ 2>/dev/null || true
mv examples/collect_provenance.py archive/old_examples/ 2>/dev/null || true
mv examples/mock_provenance_generation.py archive/old_examples/ 2>/dev/null || true
mv examples/create_modified_template.py archive/old_examples/ 2>/dev/null || true
mv examples/upload_and_run_template.py archive/old_examples/ 2>/dev/null || true
mv examples/walk_provenance.py archive/old_examples/ 2>/dev/null || true

# Old generation scripts (replaced by provenance_to_python.py)
mv examples/generate_from_provenance.py archive/old_examples/ 2>/dev/null || true
mv examples/generated_invokehttp_flow.py archive/old_examples/ 2>/dev/null || true

# ============================================================================
# Move test data to test-data/
# ============================================================================
echo "Moving test data to test-data/..."

# XML templates
mv examples/*.xml test-data/ 2>/dev/null || true

# JSON output files
mv examples/*.json test-data/ 2>/dev/null || true

# CSV files
mv examples/*.csv test-data/ 2>/dev/null || true

# ============================================================================
# Move documentation from examples/ to archive
# ============================================================================
echo "Moving old documentation to archive/old_docs/..."

mv examples/*.md archive/old_docs/ 2>/dev/null || true

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
echo "Cleanup complete!"
echo "============================================"
echo ""
echo "Examples directory now contains:"
ls -1 examples/*.py 2>/dev/null | sed 's/^/  /'
echo ""
echo "Archived items:"
echo "  - $(ls archive/old_examples/*.py 2>/dev/null | wc -l | xargs) old scripts in archive/old_examples/"
echo "  - $(ls archive/old_docs/*.md 2>/dev/null | wc -l | xargs) old docs in archive/old_docs/"
echo "  - $(ls test-data/* 2>/dev/null | wc -l | xargs) data files in test-data/"
echo ""
echo "✓ Project is now clean and shareable!"
