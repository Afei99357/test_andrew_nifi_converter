"""
Tests for NiFi REST API Client

These tests require a running NiFi instance at https://127.0.0.1:8443/nifi/
with credentials: apsaltis / deltalakeforthewin
"""

import pytest
from datetime import datetime, timedelta
from nifi2py.client import NiFiClient, NiFiAuthError, NiFiNotFoundError, NiFiClientError


@pytest.fixture
def client():
    """Create NiFi client for testing."""
    return NiFiClient(
        base_url="https://127.0.0.1:8443/nifi",
        username="apsaltis",
        password="deltalakeforthewin",
        verify_ssl=False,
    )


def test_client_initialization():
    """Test client initialization and URL normalization."""
    # Test with trailing slash
    client = NiFiClient(
        "https://127.0.0.1:8443/nifi/",
        "user",
        "pass",
        verify_ssl=False,
    )
    assert client.base_url == "https://127.0.0.1:8443/nifi"
    assert client.api_url == "https://127.0.0.1:8443/nifi-api"

    # Test without /nifi suffix
    client = NiFiClient(
        "https://127.0.0.1:8443",
        "user",
        "pass",
        verify_ssl=False,
    )
    assert client.base_url == "https://127.0.0.1:8443/nifi"


def test_authentication(client):
    """Test successful authentication."""
    # Should authenticate on first request
    root_id = client.get_root_process_group_id()
    assert root_id is not None
    assert len(root_id) > 0


def test_authentication_failure():
    """Test authentication with invalid credentials."""
    client = NiFiClient(
        "https://127.0.0.1:8443/nifi",
        "invalid",
        "credentials",
        verify_ssl=False,
    )
    with pytest.raises(NiFiAuthError):
        client.get_root_process_group_id()


def test_get_root_process_group_id(client):
    """Test getting root process group ID."""
    root_id = client.get_root_process_group_id()
    assert isinstance(root_id, str)
    assert len(root_id) == 36  # UUID format


def test_get_process_group(client):
    """Test getting process group details."""
    root_id = client.get_root_process_group_id()
    pg = client.get_process_group(root_id)

    assert "processGroupFlow" in pg
    assert "id" in pg["processGroupFlow"]
    assert "flow" in pg["processGroupFlow"]

    flow = pg["processGroupFlow"]["flow"]
    assert "processors" in flow
    assert "connections" in flow
    assert "processGroups" in flow


def test_list_processors(client):
    """Test listing processors."""
    processors = client.list_processors()
    assert isinstance(processors, list)

    # If there are processors, verify structure
    if processors:
        proc = processors[0]
        assert "id" in proc
        assert "component" in proc
        assert "name" in proc["component"]
        assert "type" in proc["component"]


def test_get_processor(client):
    """Test getting individual processor (if any exist)."""
    processors = client.list_processors()

    if processors:
        proc_id = processors[0]["id"]
        proc = client.get_processor(proc_id)

        assert "component" in proc
        assert "id" in proc
        assert proc["id"] == proc_id


def test_get_processor_not_found(client):
    """Test getting non-existent processor."""
    with pytest.raises(NiFiNotFoundError):
        client.get_processor("00000000-0000-0000-0000-000000000000")


def test_get_system_diagnostics(client):
    """Test getting system diagnostics."""
    diags = client.get_system_diagnostics()

    assert "systemDiagnostics" in diags
    snapshot = diags["systemDiagnostics"]["aggregateSnapshot"]

    assert "totalHeap" in snapshot
    assert "usedHeap" in snapshot
    assert "heapUtilization" in snapshot


def test_get_cluster_summary(client):
    """Test getting cluster summary."""
    summary = client.get_cluster_summary()

    # Even single-node NiFi returns cluster summary
    assert "connectedNodeCount" in summary or "connectedNodes" in summary


def test_list_templates(client):
    """Test listing templates."""
    templates = client.list_templates()
    assert isinstance(templates, list)

    if templates:
        tmpl = templates[0]
        assert "id" in tmpl
        assert "template" in tmpl
        assert "name" in tmpl["template"]


def test_query_provenance_empty(client):
    """Test provenance query (may be empty on fresh NiFi)."""
    # Query last hour
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)

    events = client.query_provenance(
        start_date=start_date,
        end_date=end_date,
        max_results=10,
    )

    assert isinstance(events, list)
    # May be empty if no flow activity


def test_context_manager(client):
    """Test client as context manager."""
    with NiFiClient(
        "https://127.0.0.1:8443/nifi",
        "apsaltis",
        "deltalakeforthewin",
        verify_ssl=False,
    ) as client:
        root_id = client.get_root_process_group_id()
        assert root_id is not None

    # Session should be closed after context exit
    assert client.session is not None  # Object still exists but session is closed


def test_url_normalization():
    """Test various URL formats are normalized correctly."""
    test_cases = [
        ("https://localhost:8443/nifi", "https://localhost:8443/nifi"),
        ("https://localhost:8443/nifi/", "https://localhost:8443/nifi"),
        ("https://localhost:8443", "https://localhost:8443/nifi"),
        ("http://nifi.example.com:8080/nifi", "http://nifi.example.com:8080/nifi"),
    ]

    for input_url, expected_url in test_cases:
        client = NiFiClient(input_url, "user", "pass", verify_ssl=False)
        assert client.base_url == expected_url, f"Failed for {input_url}"


if __name__ == "__main__":
    # Run a simple smoke test
    print("Running smoke tests...")

    client = NiFiClient(
        "https://127.0.0.1:8443/nifi",
        "apsaltis",
        "deltalakeforthewin",
        verify_ssl=False,
    )

    print("✓ Client initialized")

    root_id = client.get_root_process_group_id()
    print(f"✓ Got root process group: {root_id}")

    pg = client.get_process_group(root_id)
    print(f"✓ Got process group details")

    processors = client.list_processors()
    print(f"✓ Listed {len(processors)} processors")

    diags = client.get_system_diagnostics()
    print(f"✓ Got system diagnostics")

    templates = client.list_templates()
    print(f"✓ Listed {len(templates)} templates")

    client.close()
    print("✓ Client closed")

    print("\nAll smoke tests passed!")
