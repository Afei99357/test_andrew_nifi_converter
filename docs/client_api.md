# NiFi REST API Client Documentation

## Overview

The `NiFiClient` class provides a comprehensive Python interface to Apache NiFi's REST API. It handles authentication, connection pooling, retry logic, and error handling automatically.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from nifi2py.client import NiFiClient

# Create client
client = NiFiClient(
    base_url="https://localhost:8443/nifi",
    username="admin",
    password="password",
    verify_ssl=False  # For self-signed certificates
)

# Get root process group
root_id = client.get_root_process_group_id()

# List all processors
processors = client.list_processors()

# Clean up
client.close()
```

## Using as Context Manager

```python
with NiFiClient("https://localhost:8443/nifi", "admin", "password") as client:
    processors = client.list_processors()
    # Client automatically closed on exit
```

## API Reference

### Connection & Authentication

#### `__init__(base_url, username, password, verify_ssl=False, timeout=30, max_retries=3)`

Create a new NiFi client instance.

**Parameters:**
- `base_url` (str): NiFi base URL (e.g., "https://localhost:8443/nifi")
- `username` (str): NiFi username
- `password` (str): NiFi password
- `verify_ssl` (bool): Whether to verify SSL certificates (default: False)
- `timeout` (int): Request timeout in seconds (default: 30)
- `max_retries` (int): Maximum retry attempts (default: 3)

**Authentication:**
The client automatically handles authentication using:
1. Token-based auth (NiFi 1.14+) - preferred
2. HTTP Basic Auth - fallback

**Example:**
```python
client = NiFiClient(
    "https://nifi.example.com:8443/nifi",
    username="admin",
    password="secret",
    verify_ssl=True,  # Use True for valid SSL certs
    timeout=60,
    max_retries=5
)
```

### Flow Structure Methods

#### `get_root_process_group_id() -> str`

Get the root process group ID.

**Returns:** Root process group ID (UUID string)

**Example:**
```python
root_id = client.get_root_process_group_id()
# Returns: "757e4640-019b-1000-1651-2ad88c059d3f"
```

#### `get_process_group(group_id: str) -> Dict[str, Any]`

Get detailed process group information including processors, connections, and child groups.

**Parameters:**
- `group_id` (str): Process group ID (use "root" for root group)

**Returns:** Dictionary containing:
- `processGroupFlow`: Flow details
  - `flow`: Contains processors, connections, processGroups, etc.
  - `id`: Process group ID
  - `breadcrumb`: Navigation breadcrumb

**Example:**
```python
pg = client.get_process_group("root")
flow = pg["processGroupFlow"]["flow"]

print(f"Processors: {len(flow['processors'])}")
print(f"Connections: {len(flow['connections'])}")
print(f"Child Groups: {len(flow['processGroups'])}")
```

#### `get_processor(processor_id: str) -> Dict[str, Any]`

Get detailed processor configuration.

**Parameters:**
- `processor_id` (str): Processor ID

**Returns:** Dictionary containing processor component, config, and status

**Raises:** `NiFiNotFoundError` if processor doesn't exist

**Example:**
```python
proc = client.get_processor("abc-123")
config = proc["component"]["config"]
properties = config["properties"]

print(f"Name: {proc['component']['name']}")
print(f"Type: {proc['component']['type']}")
print(f"State: {proc['status']['runStatus']}")
```

#### `get_connection(connection_id: str) -> Dict[str, Any]`

Get connection details.

**Parameters:**
- `connection_id` (str): Connection ID

**Returns:** Dictionary with source, destination, and relationship info

**Example:**
```python
conn = client.get_connection("conn-123")
print(f"From: {conn['component']['source']['name']}")
print(f"To: {conn['component']['destination']['name']}")
print(f"Relationships: {conn['component']['selectedRelationships']}")
```

#### `list_processors(group_id: Optional[str] = None) -> List[Dict[str, Any]]`

List all processors in a process group and its children (recursive).

**Parameters:**
- `group_id` (str, optional): Process group ID (defaults to root)

**Returns:** List of processor dictionaries

**Example:**
```python
processors = client.list_processors()

for proc in processors:
    comp = proc["component"]
    print(f"{comp['name']} ({comp['type'].split('.')[-1]})")

    # Access properties
    if "config" in comp and "properties" in comp["config"]:
        for key, value in comp["config"]["properties"].items():
            print(f"  {key}: {value}")
```

### Provenance Methods

#### `query_provenance(processor_id=None, start_date=None, end_date=None, max_results=1000) -> List[Dict[str, Any]]`

Query provenance events with optional filters.

**Parameters:**
- `processor_id` (str, optional): Filter by processor ID
- `start_date` (datetime, optional): Start date for query
- `end_date` (datetime, optional): End date for query
- `max_results` (int): Maximum results to return (default: 1000)

**Returns:** List of provenance event dictionaries

**Note:** Requires appropriate NiFi permissions. May return 403 if user doesn't have provenance query rights.

**Example:**
```python
from datetime import datetime, timedelta

# Query last 24 hours
end_date = datetime.now()
start_date = end_date - timedelta(hours=24)

events = client.query_provenance(
    processor_id="abc-123",
    start_date=start_date,
    end_date=end_date,
    max_results=100
)

for event in events:
    print(f"Event {event['eventId']}: {event['eventType']}")
    print(f"  Component: {event['componentName']}")
    print(f"  Time: {event['eventTime']}")
    print(f"  FlowFile: {event['flowFileUuid']}")
```

#### `get_provenance_event(event_id: int) -> Dict[str, Any]`

Get detailed provenance event information.

**Parameters:**
- `event_id` (int): Provenance event ID

**Returns:** Dictionary with event details, attributes, and metadata

**Example:**
```python
event = client.get_provenance_event(12345)
prov_event = event["provenanceEvent"]

print(f"Type: {prov_event['eventType']}")
print(f"Component: {prov_event['componentName']}")

# Access flowfile attributes
for key, value in prov_event["attributes"].items():
    print(f"  {key}: {value}")
```

#### `get_provenance_content(event_id: int, direction: str = "output") -> bytes`

Get content from a provenance event.

**Parameters:**
- `event_id` (int): Provenance event ID
- `direction` (str): "input" or "output"

**Returns:** Raw content bytes

**Raises:**
- `ValueError` if direction is invalid
- `NiFiClientError` if content is not available (expired or not stored)

**Example:**
```python
# Get output content
content = client.get_provenance_content(12345, "output")
text = content.decode('utf-8')
print(text)

# Get input content
input_content = client.get_provenance_content(12345, "input")
```

### Template Operations

#### `list_templates() -> List[Dict[str, Any]]`

List all available templates.

**Returns:** List of template metadata dictionaries

**Example:**
```python
templates = client.list_templates()

for tmpl in templates:
    template = tmpl["template"]
    print(f"{template['name']}")
    print(f"  Description: {template.get('description', 'N/A')}")
    print(f"  ID: {tmpl['id']}")
```

#### `download_template(template_id: str) -> str`

Download template as XML.

**Parameters:**
- `template_id` (str): Template ID

**Returns:** Template XML content as string

**Example:**
```python
template_xml = client.download_template("abc-123")

with open("template.xml", "w") as f:
    f.write(template_xml)
```

#### `upload_template(template_xml: str, process_group_id: Optional[str] = None) -> str`

Upload a template to NiFi.

**Parameters:**
- `template_xml` (str): Template XML content
- `process_group_id` (str, optional): Target process group (defaults to root)

**Returns:** Template ID

**Example:**
```python
with open("template.xml", "r") as f:
    template_xml = f.read()

template_id = client.upload_template(template_xml)
print(f"Uploaded template: {template_id}")
```

#### `instantiate_template(template_id: str, process_group_id: Optional[str] = None, origin_x: float = 0.0, origin_y: float = 0.0) -> str`

Instantiate a template in a process group.

**Parameters:**
- `template_id` (str): Template ID to instantiate
- `process_group_id` (str, optional): Target process group (defaults to root)
- `origin_x` (float): X coordinate for placement
- `origin_y` (float): Y coordinate for placement

**Returns:** Flow ID of instantiated template

**Example:**
```python
flow_id = client.instantiate_template(
    template_id="abc-123",
    origin_x=100,
    origin_y=200
)
```

#### `delete_template(template_id: str) -> bool`

Delete a template.

**Parameters:**
- `template_id` (str): Template ID to delete

**Returns:** True if successful

**Example:**
```python
success = client.delete_template("abc-123")
```

### System Information Methods

#### `get_system_diagnostics() -> Dict[str, Any]`

Get system diagnostics including memory, CPU, and storage.

**Returns:** Dictionary with system diagnostics

**Example:**
```python
diags = client.get_system_diagnostics()
snapshot = diags["systemDiagnostics"]["aggregateSnapshot"]

print(f"Total Heap: {snapshot['totalHeap']}")
print(f"Used Heap: {snapshot['usedHeap']}")
print(f"Heap Utilization: {snapshot['heapUtilization']}")
print(f"Total Threads: {snapshot['totalThreads']}")

# Storage info
for repo in snapshot["contentRepositoryStorageUsage"]:
    print(f"{repo['identifier']}: {repo['freeSpace']} free")
```

#### `get_cluster_summary() -> Dict[str, Any]`

Get cluster summary information.

**Returns:** Dictionary with cluster status

**Example:**
```python
summary = client.get_cluster_summary()
print(f"Clustered: {summary.get('clustered', False)}")
print(f"Connected Nodes: {summary.get('connectedNodeCount', 0)}")
```

## Error Handling

The client defines three exception types:

### `NiFiClientError`
Base exception for all NiFi client errors.

### `NiFiAuthError`
Raised when authentication fails (401 responses).

### `NiFiNotFoundError`
Raised when a resource is not found (404 responses).

**Example:**
```python
from nifi2py.client import NiFiClient, NiFiAuthError, NiFiNotFoundError, NiFiClientError

try:
    client = NiFiClient("https://localhost:8443/nifi", "user", "pass")
    proc = client.get_processor("invalid-id")
except NiFiAuthError as e:
    print(f"Authentication failed: {e}")
except NiFiNotFoundError as e:
    print(f"Resource not found: {e}")
except NiFiClientError as e:
    print(f"API error: {e}")
```

## Retry Logic

The client automatically retries failed requests with exponential backoff:

- **Retry attempts:** Configurable via `max_retries` (default: 3)
- **Backoff factor:** 1 second (1s, 2s, 4s, 8s...)
- **Status codes retried:** 429, 500, 502, 503, 504
- **Methods retried:** All HTTP methods

## Connection Pooling

The client uses `requests.Session` with connection pooling:

- **Pool connections:** 10
- **Pool max size:** 20
- **Persistent connections:** Reused across requests
- **SSL verification:** Configurable per instance

## Best Practices

### 1. Use Context Manager
```python
with NiFiClient(url, user, pass) as client:
    # Client automatically closed
    processors = client.list_processors()
```

### 2. Handle Permissions
```python
try:
    events = client.query_provenance()
except NiFiClientError as e:
    if "403" in str(e):
        print("User lacks provenance query permissions")
```

### 3. Cache Root Process Group ID
```python
root_id = client.get_root_process_group_id()
# Reuse root_id instead of calling repeatedly
```

### 4. Paginate Large Results
```python
# Query provenance in chunks
batch_size = 100
all_events = []

for offset in range(0, 1000, batch_size):
    events = client.query_provenance(max_results=batch_size)
    all_events.extend(events)
```

### 5. Use Logging
```python
import logging

logging.basicConfig(level=logging.DEBUG)
# Client will log all API calls
```

## Common Use Cases

### Get All Processor Configurations
```python
processors = client.list_processors()

for proc in processors:
    proc_id = proc["id"]
    detailed = client.get_processor(proc_id)
    config = detailed["component"]["config"]

    print(f"{config['name']}:")
    for key, value in config["properties"].items():
        print(f"  {key} = {value}")
```

### Validate Data Flow with Provenance
```python
from datetime import datetime, timedelta

# Get provenance for last hour
events = client.query_provenance(
    processor_id="processor-id",
    start_date=datetime.now() - timedelta(hours=1),
    max_results=100
)

for event in events:
    # Get input content
    input_data = client.get_provenance_content(event["eventId"], "input")

    # Get output content
    output_data = client.get_provenance_content(event["eventId"], "output")

    # Compare with Python implementation
    # ... validation logic ...
```

### Export/Import Templates
```python
# Export template
template_xml = client.download_template("source-template-id")

# Modify if needed
# modified_xml = transform(template_xml)

# Upload to different NiFi instance
other_client = NiFiClient("https://other-nifi:8443/nifi", ...)
new_template_id = other_client.upload_template(template_xml)

# Instantiate
flow_id = other_client.instantiate_template(new_template_id)
```

## Troubleshooting

### SSL Certificate Errors
```python
# Disable SSL verification for self-signed certs
client = NiFiClient(url, user, pass, verify_ssl=False)
```

### Authentication Issues
```python
# Check NiFi version - token auth requires 1.14+
# Older versions fall back to basic auth automatically

# Enable debug logging to see auth attempts
import logging
logging.basicConfig(level=logging.DEBUG)
```

### 403 Forbidden on Provenance
```
Provenance queries require specific permissions.
Grant "query provenance" permission to the user in NiFi Access Policies.
```

### Timeout Errors
```python
# Increase timeout for slow NiFi instances
client = NiFiClient(url, user, pass, timeout=120)
```

## API Version Compatibility

The client is compatible with:
- NiFi 1.14.0+  (token auth)
- NiFi 1.0.0+   (basic auth)

Tested against: NiFi 1.28.1
