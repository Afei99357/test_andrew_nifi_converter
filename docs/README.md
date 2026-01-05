# Documentation

Technical documentation for nifi2py internals.

## Available Documentation

### [Client API](client_api.md)
REST API client documentation for interacting with NiFi.

**Topics:**
- Connection management
- Provenance queries
- Processor configuration retrieval
- Content fetching

---

### [Expression Language Transpiler Design](el_transpiler_design.md)
Design and implementation of the NiFi Expression Language (EL) to Python transpiler.

**Topics:**
- EL syntax analysis
- AST-based transpilation
- Supported functions and method chaining
- Edge cases and limitations

---

### [EL Functions Catalog](el_functions_catalog.md)
Complete catalog of supported Expression Language functions and their Python equivalents.

**Topics:**
- String functions
- Date/time functions
- Boolean logic
- Mathematical operations
- Attribute access patterns

---

### [Provenance Extraction](provenance_extraction.md)
How nifi2py extracts execution information from NiFi provenance data.

**Topics:**
- Provenance event types
- FlowFile lineage tracing
- Execution path discovery
- Processor configuration mapping

---

## Quick Links

- **Getting Started:** See [../README.md](../README.md)
- **Examples:** See [../examples/](../examples/)
- **Validation:** See [../VALIDATION_GUIDE.md](../VALIDATION_GUIDE.md)
