# nifi2py - Claude Code Project Bootstrap

## Quick Start for Claude Code

```bash
# Create the project
mkdir nifi2py && cd nifi2py
claude

# Then tell Claude:
"Initialize a Python project called nifi2py for converting NiFi flows to Python. 
Read the CLAUDE.md file for architecture and requirements."
```

---

## Project Overview

**Goal:** Convert Apache NiFi dataflows to equivalent Python code with validation capabilities.

**Key Insight:** Template parsing alone won't work for validation. Template IDs don't match provenance repository IDs. The architecture must be **provenance-driven**:

```
Provenance Event → Get Processor ID → REST API fetch config → Extract EL → Transpile → Validate
```

---

## Architecture Decision Record

### ADR-001: Provenance-Driven vs Template-Driven

**Decision:** Use provenance + REST API as the source of truth, not templates.

**Rationale:**
- Template IDs are regenerated on import (won't match live flow)
- Provenance contains actual I/O data for validation
- REST API provides live processor configs with resolved variables
- Templates useful only for static analysis/documentation

### ADR-002: Expression Language Transpiler

**Decision:** Build a dedicated EL parser/transpiler using Lark grammar.

**Rationale:**
- NiFi EL has complex syntax: `${attr:func():func()}`
- Static regex replacement is insufficient
- Need proper AST for method chaining
- Must handle embedded expressions: `"prefix_${attr}_suffix"`

### ADR-003: Processor Converter Registry

**Decision:** Plugin-based converter registry with stub fallback.

**Rationale:**
- New processor types can be added without core changes
- Unsupported processors generate TODO stubs
- Coverage tracking per flow

---

## Core Components to Build

### 1. NiFi Client (`nifi2py/client.py`)
```python
# Connect to NiFi REST API
client = NiFiClient("http://localhost:8080/nifi-api", username="admin", password="admin")

# Get flow structure
flow = client.get_flow_graph()

# Query provenance
events = client.query_provenance(processor_id="abc-123", max_results=100)

# Get content from provenance event
content = client.get_provenance_content(event_id=12345, direction="output")
```

### 2. Expression Language Transpiler (`nifi2py/expression_language.py`)
```python
transpiler = ELTranspiler()

# Simple attribute
transpiler.transpile("${filename}")  
# → "attributes.get('filename', '')"

# Method chain
transpiler.transpile("${filename:substringBefore('.'):toUpper()}")
# → "attributes.get('filename', '').split('.')[0].upper()"

# Embedded
transpiler.transpile("file_${uuid()}_${now():format('yyyyMMdd')}.txt")
# → "'file_' + str(uuid.uuid4()) + '_' + datetime.now().strftime('%Y%m%d') + '.txt'"
```

### 3. Processor Converters (`nifi2py/converters.py`)
```python
@register_converter
class UpdateAttributeConverter(ProcessorConverter):
    processor_types = ["UpdateAttribute"]
    
    def convert(self, processor: Processor) -> ConversionResult:
        # Generate Python function that replicates processor behavior
        ...
```

### 4. Code Generator (`nifi2py/generator.py`)
```python
generator = CodeGenerator(output_format="notebook")  # or "module"
result = generator.generate(flow)

for path, content in result.files.items():
    Path(path).write_text(content)
```

### 5. Validation Engine (`nifi2py/validator.py`)
```python
validator = Validator(nifi_client, python_module)

# Capture I/O from NiFi provenance
# Run same input through Python code
# Compare outputs byte-for-byte
results = validator.validate(sample_size=100)
print(f"Parity: {results.parity_percentage}%")
```

---

## Example Template for Testing

Save this as `example_flow.xml` to test the parser:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<template encoding-version="1.3">
    <description>Demo flow for nifi2py testing</description>
    <groupId>demo-group</groupId>
    <n>Demo Data Pipeline</n>
    <snippet>
        <processors>
            <id>gen-1</id>
            <parentGroupId>demo-group</parentGroupId>
            <position><x>100</x><y>100</y></position>
            <n>Generate Test Data</n>
            <type>org.apache.nifi.processors.standard.GenerateFlowFile</type>
            <config>
                <properties>
                    <entry><key>Custom Text</key><value>{"id": 1, "ts": "${now()}"}</value></entry>
                </properties>
            </config>
            <state>RUNNING</state>
            <relationships><n>success</n><autoTerminate>false</autoTerminate></relationships>
        </processors>
        
        <processors>
            <id>update-1</id>
            <parentGroupId>demo-group</parentGroupId>
            <position><x>100</x><y>200</y></position>
            <n>Add Metadata</n>
            <type>org.apache.nifi.processors.attributes.UpdateAttribute</type>
            <config>
                <properties>
                    <entry><key>processed_at</key><value>${now():format('yyyy-MM-dd HH:mm:ss')}</value></entry>
                    <entry><key>filename</key><value>data_${now():format('yyyyMMdd')}.json</value></entry>
                </properties>
            </config>
            <state>RUNNING</state>
            <relationships><n>success</n><autoTerminate>false</autoTerminate></relationships>
        </processors>
        
        <processors>
            <id>route-1</id>
            <parentGroupId>demo-group</parentGroupId>
            <position><x>100</x><y>300</y></position>
            <n>Route by Type</n>
            <type>org.apache.nifi.processors.standard.RouteOnAttribute</type>
            <config>
                <properties>
                    <entry><key>Routing Strategy</key><value>Route to Property name</value></entry>
                    <entry><key>is_json</key><value>${filename:endsWith('.json')}</value></entry>
                    <entry><key>is_csv</key><value>${filename:endsWith('.csv')}</value></entry>
                </properties>
            </config>
            <state>RUNNING</state>
            <relationships><n>is_json</n><autoTerminate>false</autoTerminate></relationships>
            <relationships><n>is_csv</n><autoTerminate>false</autoTerminate></relationships>
            <relationships><n>unmatched</n><autoTerminate>true</autoTerminate></relationships>
        </processors>
        
        <processors>
            <id>log-1</id>
            <parentGroupId>demo-group</parentGroupId>
            <position><x>100</x><y>400</y></position>
            <n>Log Complete</n>
            <type>org.apache.nifi.processors.standard.LogMessage</type>
            <config>
                <properties>
                    <entry><key>log-level</key><value>INFO</value></entry>
                    <entry><key>Log message</key><value>Processed: ${filename}</value></entry>
                </properties>
            </config>
            <state>RUNNING</state>
            <relationships><n>success</n><autoTerminate>true</autoTerminate></relationships>
        </processors>
        
        <processors>
            <id>exec-1</id>
            <parentGroupId>demo-group</parentGroupId>
            <position><x>300</x><y>300</y></position>
            <n>Run Impala Query</n>
            <type>org.apache.nifi.processors.standard.ExecuteStreamCommand</type>
            <config>
                <properties>
                    <entry><key>Command Path</key><value>/bin/impala-shell</value></entry>
                    <entry><key>Command Arguments</key><value>-q "${query}"</value></entry>
                </properties>
            </config>
            <state>STOPPED</state>
            <relationships><n>output stream</n><autoTerminate>false</autoTerminate></relationships>
            <relationships><n>original</n><autoTerminate>true</autoTerminate></relationships>
            <relationships><n>nonzero status</n><autoTerminate>false</autoTerminate></relationships>
        </processors>
        
        <connections>
            <id>conn-1</id>
            <source><id>gen-1</id><type>PROCESSOR</type></source>
            <destination><id>update-1</id><type>PROCESSOR</type></destination>
            <selectedRelationships>success</selectedRelationships>
        </connections>
        
        <connections>
            <id>conn-2</id>
            <source><id>update-1</id><type>PROCESSOR</type></source>
            <destination><id>route-1</id><type>PROCESSOR</type></destination>
            <selectedRelationships>success</selectedRelationships>
        </connections>
        
        <connections>
            <id>conn-3</id>
            <source><id>route-1</id><type>PROCESSOR</type></source>
            <destination><id>log-1</id><type>PROCESSOR</type></destination>
            <selectedRelationships>is_json</selectedRelationships>
        </connections>
        
        <connections>
            <id>conn-4</id>
            <source><id>route-1</id><type>PROCESSOR</type></source>
            <destination><id>exec-1</id><type>PROCESSOR</type></destination>
            <selectedRelationships>is_csv</selectedRelationships>
        </connections>
    </snippet>
</template>
```

---

## Processor Priority (Based on Client Analysis)

| Processor | Count | Priority | Notes |
|-----------|-------|----------|-------|
| LogMessage | 152 | P1 | Simple logging |
| UpdateAttribute | 122 | P1 | Requires EL transpiler |
| ExecuteStreamCommand | 55 | P1 | Stub + Impala→Databricks |
| RouteOnAttribute | 43 | P1 | Conditional routing |
| Wait/Notify | 34 | P2 | State management |
| ControlRate | 28 | P3 | Usually not needed in batch |
| GenerateFlowFile | 26 | P1 | Flow triggers |
| SplitContent | 3 | P2 | Content splitting |
| ExtractText | 3 | P2 | Regex extraction |
| ReplaceText | 2 | P2 | Text transformation |
| PutHDFS | 1 | P1 | → dbutils.fs |
| PutSFTP | 1 | P2 | → paramiko |

---

## EL Function Coverage (Minimum Viable)

### String Functions
- `substring(start, end)` → `str[start:end]`
- `substringBefore(s)` → `str.split(s)[0]`
- `substringAfter(s)` → `str.split(s, 1)[1]`
- `toUpper()` → `str.upper()`
- `toLower()` → `str.lower()`
- `trim()` → `str.strip()`
- `replace(a, b)` → `str.replace(a, b)`
- `replaceAll(regex, b)` → `re.sub(regex, b, str)`
- `length()` → `len(str)`
- `contains(s)` → `s in str`
- `startsWith(s)` → `str.startswith(s)`
- `endsWith(s)` → `str.endswith(s)`

### Date Functions
- `now()` → `datetime.now()`
- `format(pattern)` → `strftime(python_pattern)`

### Logic Functions
- `isEmpty()` → `not bool(str)`
- `equals(s)` → `str == s`
- `not()` → `not expr`
- `or(expr)` → `expr or expr`
- `and(expr)` → `expr and expr`
- `ifElse(a, b)` → `a if cond else b`

### Special
- `uuid()` → `str(uuid.uuid4())`
- `literal(s)` → `s`

---

## Testing Strategy

### Unit Tests
```python
# Test EL transpiler
def test_el_substring_before():
    result = transpile_el("${filename:substringBefore('.')}")
    assert result == "attributes.get('filename', '').split('.')[0]"

# Test processor conversion
def test_update_attribute_conversion():
    proc = Processor(id="test", name="Test", type="UpdateAttribute", ...)
    result = convert_processor(proc)
    assert not result.is_stub
    assert "def process_test" in result.function_code
```

### Integration Tests
```python
# Parse template → Generate code → Execute → Compare
def test_end_to_end():
    flow = parse_template("example_flow.xml")
    result = generate_flow_code(flow)
    
    # Execute generated Python with test input
    exec(result.files["example_flow.py"])
    output = run_flow([test_flowfile], "gen-1")
    
    # Validate output
    assert output[0].attributes["filename"].endswith(".json")
```

### Validation Tests (Requires Live NiFi)
```python
def test_parity_with_nifi():
    client = NiFiClient(NIFI_URL, username=USER, password=PASS)
    
    # Get provenance samples
    events = client.query_provenance(processor_id=PROC_ID, max_results=10)
    
    for event in events:
        nifi_input = client.get_provenance_content(event.event_id, "input")
        nifi_output = client.get_provenance_content(event.event_id, "output")
        
        # Run through Python
        python_output = process_function(FlowFile(content=nifi_input))
        
        assert python_output.content == nifi_output
```

---

## File Structure

```
nifi2py/
├── pyproject.toml
├── README.md
├── CLAUDE.md              # This file - for Claude Code
├── nifi2py/
│   ├── __init__.py
│   ├── models.py          # FlowFile, Processor, Connection, FlowGraph
│   ├── client.py          # NiFi REST API client
│   ├── template_parser.py # XML template parser
│   ├── expression_language.py  # EL transpiler
│   ├── converters.py      # Processor → Python converters
│   ├── generator.py       # Code generation (module/notebook)
│   ├── validator.py       # Parity validation engine
│   └── cli.py             # Click CLI
├── tests/
│   ├── test_el.py
│   ├── test_converters.py
│   ├── test_generator.py
│   └── test_integration.py
├── examples/
│   ├── example_flow.xml
│   └── demo.py
└── docs/
    └── architecture.md
```

---

## Commands for Claude Code

### Initialize Project
```
Create a Python project with pyproject.toml using hatch build system.
Dependencies: requests, lxml, networkx, jinja2, click, rich, pydantic, lark
Dev dependencies: pytest, pytest-cov, hypothesis, black, ruff, mypy
```

### Build EL Transpiler First
```
Build the Expression Language transpiler in nifi2py/expression_language.py.
Use Lark for parsing. Support the functions listed in the EL Function Coverage section.
Include comprehensive tests in tests/test_el.py using hypothesis for property-based testing.
```

### Build Converters
```
Build processor converters for the P1 processors listed above.
Each converter should generate a Python function that takes a FlowFile and returns
Dict[str, List[FlowFile]] mapping relationship names to output flowfiles.
```

### Generate from Client Template
```
Parse the client's ICN8_NiFi_flows_2025-05-06.xml template.
Generate a report showing:
- Total processors by type
- Conversion coverage percentage
- List of stubbed processors requiring manual implementation
- EL expressions found and their Python equivalents
```
