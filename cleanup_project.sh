#!/bin/bash
# Project cleanup script - organize files properly

echo "Cleaning up nifi2py project..."

# Create archive directory for old files
mkdir -p archive/reports
mkdir -p archive/debug_scripts

# Move report files to archive
echo "Archiving report files..."
mv CLIENT_IMPLEMENTATION_REPORT.md archive/reports/ 2>/dev/null
mv FINAL_REPORT.txt archive/reports/ 2>/dev/null
mv FINAL_SUMMARY.md archive/reports/ 2>/dev/null
mv IMPLEMENTATION_SUMMARY.md archive/reports/ 2>/dev/null
mv INDEX.md archive/reports/ 2>/dev/null
mv PARSER_ENHANCEMENT_SUMMARY.md archive/reports/ 2>/dev/null
mv PARSER_UPDATE_REPORT.md archive/reports/ 2>/dev/null
mv PROVENANCE_DRIVEN_APPROACH.md archive/reports/ 2>/dev/null
mv PROVENANCE_DRIVEN_WORKFLOW.md archive/reports/ 2>/dev/null
mv PROVENANCE_REPORT.md archive/reports/ 2>/dev/null
mv QUICK_START_PROVENANCE.md archive/reports/ 2>/dev/null
mv TEMPLATE_PARSER_REPORT.md archive/reports/ 2>/dev/null
mv executestream_commands_report.txt archive/reports/ 2>/dev/null
mv impala_to_databricks_migration.md archive/reports/ 2>/dev/null

# Move debug scripts to archive
echo "Archiving debug scripts..."
mv analyze_template_structure.py archive/debug_scripts/ 2>/dev/null
mv cleanup_stale_provenance_queries.py archive/debug_scripts/ 2>/dev/null
mv debug_provenance_query.py archive/debug_scripts/ 2>/dev/null
mv extract_impala_queries.py archive/debug_scripts/ 2>/dev/null
mv test_auth_debug.py archive/debug_scripts/ 2>/dev/null
mv test_client_direct.py archive/debug_scripts/ 2>/dev/null
mv test_client_vs_direct.py archive/debug_scripts/ 2>/dev/null
mv test_headers_explicit.py archive/debug_scripts/ 2>/dev/null
mv test_provenance_direct.py archive/debug_scripts/ 2>/dev/null
mv test_simple_session.py archive/debug_scripts/ 2>/dev/null
mv test_token_in_session.py archive/debug_scripts/ 2>/dev/null

# Move test files to examples
echo "Moving test files to examples..."
mv test_customer_template.py examples/ 2>/dev/null
mv test_template_parser.py examples/ 2>/dev/null

# Move generated files to a generated/ directory
echo "Organizing generated files..."
mkdir -p generated
mv generated_from_provenance.py generated/ 2>/dev/null
mv generated_mock_example.py generated/ 2>/dev/null
mv provenance_analysis.json generated/ 2>/dev/null

# Remove build artifacts
echo "Removing build artifacts..."
rm -rf .pytest_cache
rm -rf __pycache__
rm -f .coverage
rm -f .DS_Store
rm -f nifi2py-prd.docx

# Clean up Python cache in subdirectories
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null
find . -type f -name "*.pyc" -delete 2>/dev/null

echo "✓ Cleanup complete!"
echo ""
echo "Project structure:"
echo "  nifi2py/          - Core library"
echo "  examples/         - Example scripts"
echo "  tests/            - Unit tests"
echo "  generated/        - Generated code output"
echo "  archive/          - Old reports and debug scripts"
echo "  docs/             - Documentation"
