"""
NiFi REST API Client

Comprehensive client for interacting with Apache NiFi REST API.
Supports flow structure queries, provenance data retrieval, and template operations.

Example:
    >>> client = NiFiClient(
    ...     "https://localhost:8443/nifi",
    ...     username="admin",
    ...     password="admin123",
    ...     verify_ssl=False
    ... )
    >>> root_id = client.get_root_process_group_id()
    >>> processors = client.list_processors()
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from urllib3.util.retry import Retry

# Configure logging
logger = logging.getLogger(__name__)


class NiFiClientError(Exception):
    """Base exception for NiFi client errors."""
    pass


class NiFiAuthError(NiFiClientError):
    """Authentication-related errors."""
    pass


class NiFiNotFoundError(NiFiClientError):
    """Resource not found errors."""
    pass


class NiFiClient:
    """
    NiFi REST API Client

    Provides methods to interact with NiFi's REST API for:
    - Flow structure queries (process groups, processors, connections)
    - Provenance data retrieval (events and content)
    - Template operations (upload, instantiate, delete)

    Args:
        base_url: NiFi base URL (e.g., "https://localhost:8443/nifi")
        username: NiFi username for authentication
        password: NiFi password for authentication
        verify_ssl: Whether to verify SSL certificates (default: False)
        timeout: Request timeout in seconds (default: 30)
        max_retries: Maximum number of retry attempts (default: 3)

    Example:
        >>> client = NiFiClient(
        ...     "https://localhost:8443/nifi",
        ...     username="admin",
        ...     password="password"
        ... )
        >>> root_pg = client.get_root_process_group_id()
        >>> print(f"Root PG: {root_pg}")
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = False,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        """Initialize NiFi client with authentication."""
        # Normalize base URL
        self.base_url = base_url.rstrip("/")
        if not self.base_url.endswith("/nifi"):
            self.base_url += "/nifi"

        self.api_url = f"{self.base_url}-api"
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.timeout = timeout

        # Create session with retry logic
        self.session = self._create_session(max_retries)

        # Auth token (will be populated on first request if needed)
        self._auth_token: Optional[str] = None

        # Authenticate immediately to catch auth errors early
        self._authenticate()

        logger.info(f"Initialized NiFiClient for {self.base_url}")

    def _create_session(self, max_retries: int) -> requests.Session:
        """Create requests session with retry logic and connection pooling."""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,  # 1, 2, 4, 8 seconds
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=20,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Disable SSL warnings if verify_ssl is False
        if not self.verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        return session

    def _authenticate(self) -> None:
        """
        Authenticate with NiFi and obtain access token if using token-based auth.

        NiFi 1.14+ supports token-based authentication. This method attempts to
        obtain a token first, and falls back to basic auth if unavailable.

        IMPORTANT: Token request must NOT use the same session to avoid session
        state conflicts that cause 403 errors on subsequent requests.
        """
        try:
            # Try token-based authentication (NiFi 1.14+)
            token_url = f"{self.api_url}/access/token"
            logger.debug(f"Attempting token authentication at {token_url}")

            # IMPORTANT: Use requests.post (not session) to avoid session state conflicts
            response = requests.post(
                token_url,
                data={"username": self.username, "password": self.password},
                verify=self.verify_ssl,
                timeout=self.timeout,
            )

            logger.debug(f"Token auth response: {response.status_code}")

            if response.status_code == 201:
                self._auth_token = response.text
                self.session.headers.update({"Authorization": f"Bearer {self._auth_token}"})
                logger.info(f"Successfully authenticated with token-based auth (token: {self._auth_token[:20]}...)")
                return
            elif response.status_code == 404:
                # Token endpoint not available, use basic auth
                logger.info("Token endpoint not available, using basic auth")
                self.session.auth = HTTPBasicAuth(self.username, self.password)
                return
            else:
                logger.warning(f"Token auth failed with status {response.status_code}: {response.text[:200]}")
                logger.warning("Falling back to basic auth")
                self.session.auth = HTTPBasicAuth(self.username, self.password)

        except requests.RequestException as e:
            logger.warning(f"Token authentication failed: {e}, falling back to basic auth")
            self.session.auth = HTTPBasicAuth(self.username, self.password)

    def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs: Any,
    ) -> requests.Response:
        """
        Make authenticated request to NiFi API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (will be joined with api_url)
            **kwargs: Additional arguments passed to requests

        Returns:
            Response object

        Raises:
            NiFiAuthError: Authentication failed
            NiFiNotFoundError: Resource not found
            NiFiClientError: Other API errors
        """
        # Ensure we're authenticated
        if not self._auth_token and not self.session.auth:
            self._authenticate()

        url = urljoin(f"{self.api_url}/", endpoint.lstrip("/"))

        # Set defaults
        kwargs.setdefault("verify", self.verify_ssl)
        kwargs.setdefault("timeout", self.timeout)

        logger.debug(f"{method} {url}")

        try:
            response = self.session.request(method, url, **kwargs)

            # Handle common error codes
            if response.status_code == 401:
                # Try to re-authenticate once
                logger.warning("Received 401, attempting re-authentication")
                self._auth_token = None
                self.session.auth = None
                self._authenticate()

                # Retry the request
                response = self.session.request(method, url, **kwargs)
                if response.status_code == 401:
                    raise NiFiAuthError(f"Authentication failed: {response.text}")

            elif response.status_code == 404:
                raise NiFiNotFoundError(f"Resource not found: {url}")

            elif response.status_code >= 400:
                raise NiFiClientError(
                    f"API request failed: {response.status_code} - {response.text}"
                )

            response.raise_for_status()
            return response

        except requests.RequestException as e:
            if isinstance(e, requests.HTTPError):
                raise NiFiClientError(f"HTTP error: {e}") from e
            raise NiFiClientError(f"Request failed: {e}") from e

    # ========================================================================
    # Flow Structure Methods
    # ========================================================================

    def get_root_process_group_id(self) -> str:
        """
        Get the root process group ID.

        Returns:
            Root process group ID

        Example:
            >>> client = NiFiClient("https://localhost:8443/nifi", "admin", "pass")
            >>> root_id = client.get_root_process_group_id()
            >>> print(root_id)
            'a1b2c3d4-5678-90ab-cdef-1234567890ab'
        """
        response = self._request("GET", "/flow/process-groups/root")
        data = response.json()
        return data["processGroupFlow"]["id"]

    def get_process_group(self, group_id: str) -> Dict[str, Any]:
        """
        Get process group details including all processors, connections, etc.

        Args:
            group_id: Process group ID (use 'root' for root group)

        Returns:
            Process group data including processors, connections, and child groups

        Example:
            >>> pg = client.get_process_group("root")
            >>> print(f"Found {len(pg['processGroupFlow']['flow']['processors'])} processors")
        """
        response = self._request("GET", f"/flow/process-groups/{group_id}")
        return response.json()

    def get_processor(self, processor_id: str) -> Dict[str, Any]:
        """
        Get detailed processor configuration.

        Args:
            processor_id: Processor ID

        Returns:
            Processor configuration including properties, relationships, etc.

        Example:
            >>> proc = client.get_processor("abc-123")
            >>> print(proc['component']['name'])
            'UpdateAttribute'
        """
        response = self._request("GET", f"/processors/{processor_id}")
        return response.json()

    def get_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Get connection details.

        Args:
            connection_id: Connection ID

        Returns:
            Connection data including source, destination, and relationships

        Example:
            >>> conn = client.get_connection("conn-123")
            >>> print(f"{conn['source']['name']} -> {conn['destination']['name']}")
        """
        response = self._request("GET", f"/connections/{connection_id}")
        return response.json()

    def list_processors(self, group_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all processors in a process group (or root if not specified).

        Args:
            group_id: Process group ID (defaults to root)

        Returns:
            List of processor objects

        Example:
            >>> processors = client.list_processors()
            >>> for proc in processors:
            ...     print(f"{proc['id']}: {proc['component']['name']}")
        """
        if group_id is None:
            group_id = self.get_root_process_group_id()

        pg_data = self.get_process_group(group_id)
        processors = pg_data["processGroupFlow"]["flow"]["processors"]

        # Recursively get processors from child groups
        child_groups = pg_data["processGroupFlow"]["flow"]["processGroups"]
        for child in child_groups:
            processors.extend(self.list_processors(child["id"]))

        return processors

    # ========================================================================
    # Provenance Methods
    # ========================================================================

    def query_provenance(
        self,
        processor_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_results: int = 1000,
        max_events: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query provenance events with automatic pagination.

        Args:
            processor_id: Filter by processor ID (optional)
            start_date: Start date for query (optional)
            end_date: End date for query (optional)
            max_results: Results per page (default 1000, min 200 to avoid NiFi bug)
            max_events: Maximum total events to collect across all pages (optional)

        Returns:
            List of provenance events (may span multiple pages)

        Example:
            >>> # Get up to 5000 events with pagination
            >>> events = client.query_provenance(max_events=5000)
            >>> for event in events:
            ...     print(f"Event {event['eventId']}: {event['eventType']}")

        Note:
            NiFi has a bug where max_results < 200 returns 0 events.
            This method uses max_results=1000 by default.

            If max_events is specified, this method will automatically paginate
            through multiple queries to collect up to max_events total events.
        """
        # Ensure max_results is at least 200 to avoid NiFi bug
        if max_results < 200:
            logger.warning(f"max_results={max_results} too low (NiFi bug), using 1000")
            max_results = 1000

        # If max_events not specified, just do a single query
        if max_events is None:
            return self._query_provenance_single(
                processor_id=processor_id,
                start_date=start_date,
                end_date=end_date,
                max_results=max_results
            )

        # Paginate to collect up to max_events
        all_events = []
        current_end_date = end_date
        page_num = 1

        while len(all_events) < max_events:
            logger.debug(f"Fetching provenance page {page_num} (have {len(all_events)} events so far)")

            events = self._query_provenance_single(
                processor_id=processor_id,
                start_date=start_date,
                end_date=current_end_date,
                max_results=max_results
            )

            if not events:
                logger.debug("No more events available")
                break

            all_events.extend(events)
            page_num += 1

            # If we got fewer events than requested, we're done
            if len(events) < max_results:
                logger.debug(f"Got {len(events)} < {max_results}, last page reached")
                break

            # Use the oldest event's timestamp as end_date for next page
            # This ensures we get the next batch of older events
            oldest_event = events[-1]
            if "eventTime" in oldest_event:
                # Parse timestamp like "01/03/2026 20:51:38.310 MST"
                event_time_str = oldest_event["eventTime"]
                try:
                    # NiFi uses format: MM/DD/YYYY HH:MM:SS.mmm TZ
                    from datetime import datetime
                    # Remove timezone suffix for parsing
                    time_part = event_time_str.rsplit(" ", 1)[0]
                    current_end_date = datetime.strptime(time_part, "%m/%d/%Y %H:%M:%S.%f")
                    logger.debug(f"Next page will end at: {current_end_date}")
                except Exception as e:
                    logger.warning(f"Failed to parse event time '{event_time_str}': {e}")
                    break
            else:
                logger.debug("No eventTime in oldest event, stopping pagination")
                break

        result = all_events[:max_events] if max_events else all_events
        logger.info(f"Collected {len(result)} total provenance events across {page_num} pages")
        return result

    def _query_provenance_single(
        self,
        processor_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_results: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Execute a single provenance query (internal method).

        This is called by query_provenance() for each page.
        """
        # Build query request
        query_request: Dict[str, Any] = {
            "provenance": {
                "request": {
                    "maxResults": max_results,
                }
            }
        }

        # Add search terms
        search_terms = {}
        if processor_id:
            search_terms["ProcessorID"] = processor_id

        if start_date:
            # Use ISO 8601 format that NiFi can parse
            search_terms["StartDate"] = start_date.isoformat()

        if end_date:
            # Use ISO 8601 format that NiFi can parse
            search_terms["EndDate"] = end_date.isoformat()

        if search_terms:
            query_request["provenance"]["request"]["searchTerms"] = search_terms

        # Submit query
        response = self._request("POST", "/provenance", json=query_request)
        query_data = response.json()
        query_id = query_data["provenance"]["id"]
        query_url = query_data["provenance"]["uri"]

        # Poll for results
        max_attempts = 30
        for attempt in range(max_attempts):
            time.sleep(1)  # Wait before polling

            response = self._request("GET", query_url.replace(self.api_url, ""))
            result = response.json()

            if result["provenance"]["finished"]:
                events = result["provenance"]["results"]["provenanceEvents"]
                logger.info(f"Retrieved {len(events)} provenance events")

                # Clean up query (CRITICAL: prevents "poorly behaving clients" error)
                try:
                    self._request("DELETE", f"/provenance/{query_id}")
                    logger.debug(f"Cleaned up provenance query {query_id}")
                except Exception as e:
                    logger.warning(f"Failed to clean up provenance query {query_id}: {e}")

                return events

            logger.debug(f"Waiting for provenance query (attempt {attempt + 1}/{max_attempts})")

        # Clean up timed-out query
        try:
            self._request("DELETE", f"/provenance/{query_id}")
            logger.debug(f"Cleaned up timed-out provenance query {query_id}")
        except Exception as e:
            logger.warning(f"Failed to clean up timed-out query {query_id}: {e}")

        raise NiFiClientError(f"Provenance query timed out after {max_attempts} attempts")

    def get_provenance_event(self, event_id: int) -> Dict[str, Any]:
        """
        Get detailed provenance event information.

        Args:
            event_id: Provenance event ID

        Returns:
            Provenance event details

        Example:
            >>> event = client.get_provenance_event(12345)
            >>> print(event['provenanceEvent']['eventType'])
            'CONTENT_MODIFIED'
        """
        response = self._request("GET", f"/provenance/events/{event_id}")
        return response.json()

    def get_provenance_content(
        self,
        event_id: int,
        direction: str = "output",
    ) -> bytes:
        """
        Get content from a provenance event.

        Args:
            event_id: Provenance event ID
            direction: "input" or "output"

        Returns:
            Raw content bytes

        Raises:
            ValueError: If direction is not "input" or "output"

        Example:
            >>> content = client.get_provenance_content(12345, "output")
            >>> print(content.decode('utf-8'))
            '{"result": "success"}'
        """
        if direction not in ("input", "output"):
            raise ValueError(f"direction must be 'input' or 'output', got '{direction}'")

        response = self._request("GET", f"/provenance-events/{event_id}/content/{direction}")
        return response.content

    # ========================================================================
    # Template Operations
    # ========================================================================

    def upload_template(self, template_xml: str, process_group_id: Optional[str] = None) -> str:
        """
        Upload a template to NiFi.

        Args:
            template_xml: Template XML content
            process_group_id: Process group to upload to (defaults to root)

        Returns:
            Template ID

        Example:
            >>> with open("template.xml") as f:
            ...     template_id = client.upload_template(f.read())
            >>> print(f"Uploaded template: {template_id}")
        """
        if process_group_id is None:
            process_group_id = self.get_root_process_group_id()

        files = {"template": ("template.xml", template_xml, "application/xml")}

        response = self._request(
            "POST",
            f"/process-groups/{process_group_id}/templates/upload",
            files=files,
        )

        data = response.json()
        template_id = data["template"]["id"]
        logger.info(f"Uploaded template {template_id}")
        return template_id

    def instantiate_template(
        self,
        template_id: str,
        process_group_id: Optional[str] = None,
        origin_x: float = 0.0,
        origin_y: float = 0.0,
    ) -> str:
        """
        Instantiate a template in a process group.

        Args:
            template_id: Template ID to instantiate
            process_group_id: Process group to instantiate in (defaults to root)
            origin_x: X coordinate for template placement
            origin_y: Y coordinate for template placement

        Returns:
            Flow ID of instantiated template

        Example:
            >>> flow_id = client.instantiate_template(
            ...     template_id="abc-123",
            ...     origin_x=100,
            ...     origin_y=200
            ... )
        """
        if process_group_id is None:
            process_group_id = self.get_root_process_group_id()

        payload = {
            "templateId": template_id,
            "originX": origin_x,
            "originY": origin_y,
        }

        response = self._request(
            "POST",
            f"/process-groups/{process_group_id}/template-instance",
            json=payload,
        )

        data = response.json()
        flow_id = data["flow"]["id"]
        logger.info(f"Instantiated template {template_id} as flow {flow_id}")
        return flow_id

    def delete_template(self, template_id: str) -> bool:
        """
        Delete a template from NiFi.

        Args:
            template_id: Template ID to delete

        Returns:
            True if deletion was successful

        Example:
            >>> success = client.delete_template("abc-123")
            >>> print(f"Deleted: {success}")
        """
        response = self._request("DELETE", f"/templates/{template_id}")
        logger.info(f"Deleted template {template_id}")
        return response.status_code == 200

    def list_templates(self) -> List[Dict[str, Any]]:
        """
        List all available templates.

        Returns:
            List of template metadata

        Example:
            >>> templates = client.list_templates()
            >>> for tmpl in templates:
            ...     print(f"{tmpl['id']}: {tmpl['template']['name']}")
        """
        response = self._request("GET", "/flow/templates")
        data = response.json()
        return data.get("templates", [])

    def download_template(self, template_id: str) -> str:
        """
        Download a template as XML.

        Args:
            template_id: Template ID to download

        Returns:
            Template XML content

        Example:
            >>> xml = client.download_template("abc-123")
            >>> with open("template.xml", "w") as f:
            ...     f.write(xml)
        """
        response = self._request("GET", f"/templates/{template_id}/download")
        return response.text

    # ========================================================================
    # Processor Control Methods
    # ========================================================================

    def start_processor(self, processor_id: str) -> Dict[str, Any]:
        """
        Start a processor.

        Args:
            processor_id: Processor ID to start

        Returns:
            Updated processor entity

        Example:
            >>> proc = client.start_processor("abc-123")
            >>> print(proc['component']['state'])
            'RUNNING'
        """
        # First get current processor state
        current = self.get_processor(processor_id)

        # Update state to RUNNING
        payload = {
            "revision": current["revision"],
            "component": {
                "id": processor_id,
                "state": "RUNNING",
            },
        }

        response = self._request("PUT", f"/processors/{processor_id}", json=payload)
        logger.info(f"Started processor {processor_id}")
        return response.json()

    def stop_processor(self, processor_id: str) -> Dict[str, Any]:
        """
        Stop a processor.

        Args:
            processor_id: Processor ID to stop

        Returns:
            Updated processor entity

        Example:
            >>> proc = client.stop_processor("abc-123")
            >>> print(proc['component']['state'])
            'STOPPED'
        """
        # First get current processor state
        current = self.get_processor(processor_id)

        # Update state to STOPPED
        payload = {
            "revision": current["revision"],
            "component": {
                "id": processor_id,
                "state": "STOPPED",
            },
        }

        response = self._request("PUT", f"/processors/{processor_id}", json=payload)
        logger.info(f"Stopped processor {processor_id}")
        return response.json()

    def start_all_processors(
        self, process_group_id: Optional[str] = None, recursive: bool = True
    ) -> Dict[str, int]:
        """
        Start all processors in a process group.

        Args:
            process_group_id: Process group ID (defaults to root)
            recursive: Whether to start processors in child groups

        Returns:
            Dictionary with counts of started, already running, and failed processors

        Example:
            >>> results = client.start_all_processors()
            >>> print(f"Started: {results['started']}, Failed: {results['failed']}")
        """
        if process_group_id is None:
            process_group_id = self.get_root_process_group_id()

        processors = self.list_processors(process_group_id) if recursive else []
        if not recursive:
            pg_data = self.get_process_group(process_group_id)
            processors = pg_data["processGroupFlow"]["flow"]["processors"]

        results = {"started": 0, "already_running": 0, "failed": 0}

        for proc in processors:
            try:
                state = proc["component"]["state"]
                if state == "RUNNING":
                    results["already_running"] += 1
                else:
                    self.start_processor(proc["id"])
                    results["started"] += 1
            except Exception as e:
                logger.warning(f"Failed to start processor {proc['id']}: {e}")
                results["failed"] += 1

        logger.info(
            f"Processor start results: {results['started']} started, "
            f"{results['already_running']} already running, {results['failed']} failed"
        )
        return results

    def stop_all_processors(
        self, process_group_id: Optional[str] = None, recursive: bool = True
    ) -> Dict[str, int]:
        """
        Stop all processors in a process group.

        Args:
            process_group_id: Process group ID (defaults to root)
            recursive: Whether to stop processors in child groups

        Returns:
            Dictionary with counts of stopped, already stopped, and failed processors

        Example:
            >>> results = client.stop_all_processors()
            >>> print(f"Stopped: {results['stopped']}, Failed: {results['failed']}")
        """
        if process_group_id is None:
            process_group_id = self.get_root_process_group_id()

        processors = self.list_processors(process_group_id) if recursive else []
        if not recursive:
            pg_data = self.get_process_group(process_group_id)
            processors = pg_data["processGroupFlow"]["flow"]["processors"]

        results = {"stopped": 0, "already_stopped": 0, "failed": 0}

        for proc in processors:
            try:
                state = proc["component"]["state"]
                if state == "STOPPED":
                    results["already_stopped"] += 1
                else:
                    self.stop_processor(proc["id"])
                    results["stopped"] += 1
            except Exception as e:
                logger.warning(f"Failed to stop processor {proc['id']}: {e}")
                results["failed"] += 1

        logger.info(
            f"Processor stop results: {results['stopped']} stopped, "
            f"{results['already_stopped']} already stopped, {results['failed']} failed"
        )
        return results

    def get_current_user(self) -> Dict[str, Any]:
        """
        Get the current user identity.

        Returns:
            User identity information

        Example:
            >>> user = client.get_current_user()
            >>> print(f"Logged in as: {user['identity']}")
        """
        response = self._request("GET", "/flow/current-user")
        return response.json()

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def get_cluster_summary(self) -> Dict[str, Any]:
        """
        Get cluster summary information.

        Returns:
            Cluster summary including node count and status

        Example:
            >>> summary = client.get_cluster_summary()
            >>> print(f"Cluster nodes: {summary['connectedNodeCount']}")
        """
        response = self._request("GET", "/flow/cluster/summary")
        return response.json()["clusterSummary"]

    def get_system_diagnostics(self) -> Dict[str, Any]:
        """
        Get system diagnostics information.

        Returns:
            System diagnostics including heap usage, CPU, etc.

        Example:
            >>> diags = client.get_system_diagnostics()
            >>> heap = diags['systemDiagnostics']['aggregateSnapshot']['heapUtilization']
            >>> print(f"Heap usage: {heap}")
        """
        response = self._request("GET", "/system-diagnostics")
        return response.json()

    def get_provenance_event_content(
        self, event_id: str, direction: str = "output"
    ) -> bytes:
        """
        Get FlowFile content from a provenance event.

        Args:
            event_id: Provenance event ID
            direction: 'input' or 'output' (default: 'output')

        Returns:
            FlowFile content as bytes

        Raises:
            NiFiClientError: If content is not available or request fails

        Example:
            >>> content = client.get_provenance_event_content("12345", "output")
            >>> print(f"Content: {len(content)} bytes")

        Note:
            Provenance content is only retained for a configurable period.
            Older events may not have content available.
        """
        if direction not in ("input", "output"):
            raise ValueError(f"direction must be 'input' or 'output', got: {direction}")

        endpoint = f"/provenance-events/{event_id}/content/{direction}"

        try:
            response = self._request("GET", endpoint)
            return response.content
        except NiFiClientError as e:
            # Content might not be available
            raise NiFiClientError(
                f"Failed to get {direction} content for event {event_id}. "
                f"Content may not be available (too old or not retained): {e}"
            )

    def close(self) -> None:
        """Close the session and cleanup resources."""
        if self.session:
            self.session.close()
            logger.info("Closed NiFi client session")

    def __enter__(self) -> "NiFiClient":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


if __name__ == "__main__":
    # Simple test script
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("Testing NiFi Client Connection...")
    print("=" * 60)

    try:
        # Connect to NiFi
        client = NiFiClient(
            base_url="https://127.0.0.1:8443/nifi",
            username="apsaltis",
            password="deltalakeforthewin",
            verify_ssl=False,
        )

        print("\n1. Getting root process group...")
        root_id = client.get_root_process_group_id()
        print(f"   Root Process Group ID: {root_id}")

        print("\n2. Getting process group details...")
        pg = client.get_process_group(root_id)
        flow = pg["processGroupFlow"]["flow"]
        print(f"   Process Group Name: {pg['processGroupFlow']['breadcrumb']['breadcrumb']['name']}")
        print(f"   Processors: {len(flow['processors'])}")
        print(f"   Connections: {len(flow['connections'])}")
        print(f"   Process Groups: {len(flow['processGroups'])}")

        print("\n3. Listing all processors...")
        processors = client.list_processors(root_id)
        print(f"   Total processors (including nested): {len(processors)}")

        if processors:
            print("\n   Sample processors:")
            for proc in processors[:5]:
                comp = proc["component"]
                print(f"   - {comp['name']} ({comp['type'].split('.')[-1]})")

        print("\n4. Getting system diagnostics...")
        diags = client.get_system_diagnostics()
        snapshot = diags["systemDiagnostics"]["aggregateSnapshot"]
        print(f"   Total Heap: {snapshot['totalHeap']}")
        print(f"   Used Heap: {snapshot['usedHeap']}")
        print(f"   Heap Utilization: {snapshot['heapUtilization']}")

        print("\n5. Listing templates...")
        templates = client.list_templates()
        print(f"   Total templates: {len(templates)}")
        if templates:
            print("\n   Available templates:")
            for tmpl in templates:
                print(f"   - {tmpl['template']['name']} (ID: {tmpl['id']})")

        print("\n" + "=" * 60)
        print("SUCCESS! All tests passed.")
        print("=" * 60)

        client.close()
        sys.exit(0)

    except NiFiAuthError as e:
        print(f"\nAUTHENTICATION ERROR: {e}")
        sys.exit(1)
    except NiFiClientError as e:
        print(f"\nCLIENT ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nUNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
