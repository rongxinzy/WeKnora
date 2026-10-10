#!/usr/bin/env python3
"""
WeKnora MCP Server

A Model Context Protocol server that provides access to the WeKnora knowledge management API.
"""

import argparse
import asyncio
from contextlib import contextmanager
import functools
import json
import logging
import math
import os
import re
import secrets
import sys
import threading
import urllib.parse
from typing import Any, Dict

import urllib3
import requests
from mcp.server import MCPServer
from requests.exceptions import RequestException
from upload_paths import resolve_upload_file_path, set_active_transport

# Set up logging configuration for the MCP server
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration - Load from environment variables with defaults
WEKNORA_BASE_URL = os.getenv("WEKNORA_BASE_URL", "http://localhost:8080/api/v1")
WEKNORA_API_KEY = os.getenv("WEKNORA_API_KEY", "")
# Chat SSE read timeout in seconds. LLM responses can be slow; default 300s.
try:
    WEKNORA_CHAT_TIMEOUT = int(os.getenv("WEKNORA_CHAT_TIMEOUT", "300"))
except ValueError:
    logger.warning("WEKNORA_CHAT_TIMEOUT is not a valid integer; falling back to 300s.")
    WEKNORA_CHAT_TIMEOUT = 300

# Network transport defaults kept for backward compatibility with pre-2.x deployments.
SSE_MESSAGE_PATH = "/sse/messages/"
STREAMABLE_HTTP_STATELESS = True
PORTAL_KNOWLEDGE_AUTH_URL = os.getenv("PORTAL_KNOWLEDGE_AUTH_URL", "").strip().rstrip("/")
PORTAL_KNOWLEDGE_AUTH_REQUIRED = os.getenv("PORTAL_KNOWLEDGE_AUTH_REQUIRED", "false").strip().lower() == "true"
_EMPLOYEE_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$", re.ASCII)
_OPAQUE_CONTEXT_RE = re.compile(r"^[A-Za-z0-9_-]{43}$", re.ASCII)


def _validate_read_pagination(page: int, page_size: int) -> None:
    """Match WeKnora's tenant-scoped list query limits before issuing reads."""
    if isinstance(page, bool) or not isinstance(page, int) or page < 1:
        raise ValueError("page must be a positive integer")
    if isinstance(page_size, bool) or not isinstance(page_size, int) or not 1 <= page_size <= 100:
        raise ValueError("page_size must be between 1 and 100")


def _validate_hybrid_search(query: str, vector_threshold: float, keyword_threshold: float, match_count: int) -> None:
    if not isinstance(query, str) or not query.strip() or len(query) > 4000:
        raise ValueError("query must contain 1 to 4000 characters")
    for name, value in (("vector_threshold", vector_threshold), ("keyword_threshold", keyword_threshold)):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"{name} must be between 0 and 1")
    if isinstance(match_count, bool) or not isinstance(match_count, int) or not 1 <= match_count <= 100:
        raise ValueError("match_count must be between 1 and 100")


def network_transport_auth_token() -> str:
    """Shared secret clients must present for SSE/HTTP transports."""
    return os.getenv("MCP_SERVER_AUTH_TOKEN", "").strip()


def require_network_transport_auth(transport: str) -> str:
    """SSE/HTTP must not start without a configured auth token."""
    token = network_transport_auth_token()
    if transport in ("sse", "http") and not token:
        logger.error(
            "MCP_SERVER_AUTH_TOKEN is required for %s transport. "
            "Set a strong shared secret; clients must send "
            "Authorization: Bearer <token> or X-MCP-Auth-Token.",
            transport,
        )
        sys.exit(1)
    return token


class MCPAuthMiddleware:
    """ASGI middleware that gates network MCP transports behind a shared secret."""

    def __init__(self, app, token: str):
        self.app = app
        self.token = token

    async def __call__(self, scope, receive, send):
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        headers = {
            k.decode("latin-1").lower(): v.decode("latin-1")
            for k, v in scope.get("headers", [])
        }
        provided = ""
        auth = headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            provided = auth[7:].strip()
        elif "x-mcp-auth-token" in headers:
            provided = headers["x-mcp-auth-token"]

        if not provided or not secrets.compare_digest(provided, self.token):
            body = b'{"error":"unauthorized"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [[b"content-type", b"application/json"]],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return

        await self.app(scope, receive, send)


def _normalize_kb_entries(resp: object) -> list[Dict]:
    """Flatten owned and shared knowledge-base list API responses.

    GET /knowledge-bases returns ``data: [{id, name, ...}, ...]`` (see
    KnowledgeBaseHandler.buildKBListResponse).

    GET /shared-knowledge-bases returns ``data: [{knowledge_base: {id, name,
    ...}, share_id, ...}, ...]`` (see organization handler sharedKBRow).
    """
    data = resp.get("data", resp) if isinstance(resp, dict) else resp
    if isinstance(data, dict):
        data = data.get("list", data.get("items", []))
    out: list[Dict] = []
    for item in (data or []):
        if not isinstance(item, dict):
            continue
        nested = item.get("knowledge_base")
        if isinstance(nested, dict) and nested.get("id"):
            out.append(nested)
        elif item.get("id"):
            out.append(item)
    return out


class WeKnoraClient:
    """Client for interacting with WeKnora API"""

    def __init__(self, base_url: str, api_key: str, portal_knowledge_auth_url: str | None = None):
        """Initialize the WeKnora API client with base URL and authentication"""
        self.base_url = base_url
        self.api_key = api_key
        self.portal_knowledge_auth_url = (PORTAL_KNOWLEDGE_AUTH_URL if portal_knowledge_auth_url is None else portal_knowledge_auth_url).strip().rstrip("/")
        # SSL verification: enabled by default. Set WEKNORA_VERIFY_SSL=false to disable
        # (e.g. for self-signed certs in dev environments — NOT recommended for production).
        self.verify_ssl = os.getenv("WEKNORA_VERIFY_SSL", "true").lower() != "false"
        if not self.verify_ssl:
            logger.warning(
                "SSL certificate verification is DISABLED (WEKNORA_VERIFY_SSL=false). "
                "This is insecure and should not be used in production."
            )
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # MCP 2.x runs sync @mcp.tool() handlers on worker threads; use a
        # thread-local Session because requests.Session is not thread-safe.
        self._session_local = threading.local()
        self._knowledge_operation_local = threading.local()

    def authorize_knowledge_bases(
        self,
        knowledge_base_ids: list[str],
        employee_name: str = "",
        knowledge_context_handle: str = "",
        require_all: bool = True,
        action: str = "runtime_read",
    ) -> list[str] | None:
        """Ask Portal to revalidate the user/session and current KB grants.

        Returns None only when the Portal integration is explicitly not
        configured. Once required or once a context is supplied, every
        transport or authorization error is a denial.
        """
        self._knowledge_operation_local.operation = None
        if not self.portal_knowledge_auth_url:
            if PORTAL_KNOWLEDGE_AUTH_REQUIRED or employee_name or knowledge_context_handle:
                raise PermissionError("online knowledge authorization is unavailable")
            return None
        if (
            not _EMPLOYEE_NAME_RE.fullmatch(employee_name)
            or len(employee_name) > 63
            or not _OPAQUE_CONTEXT_RE.fullmatch(knowledge_context_handle)
            or not knowledge_base_ids
            or len(knowledge_base_ids) > 100
            or any(not isinstance(kb_id, str) or not kb_id or len(kb_id) > 128 for kb_id in knowledge_base_ids)
        ):
            raise PermissionError("knowledge session context and explicit scope are required")
        endpoint = f"{self.portal_knowledge_auth_url}/{urllib.parse.quote(employee_name, safe='')}/authorize"
        try:
            response = requests.post(
                endpoint,
                json={"knowledge_base_ids": knowledge_base_ids, "require_all": require_all, "action": action},
                headers={"X-DeerFlow-Knowledge-Context": knowledge_context_handle},
                timeout=5,
                allow_redirects=False,
            )
            if response.status_code != 200:
                raise PermissionError("knowledge access denied")
            payload = response.json()
            allowed = payload.get("knowledge_base_ids") if isinstance(payload, dict) else None
            if not isinstance(allowed, list) or not allowed or any(not isinstance(value, str) for value in allowed):
                raise PermissionError("knowledge access denied")
            allowed_set = set(allowed)
            if not allowed_set.issubset(set(knowledge_base_ids)):
                raise PermissionError("invalid Portal authorization response")
            if require_all and any(kb_id not in allowed_set for kb_id in knowledge_base_ids):
                raise PermissionError("knowledge access denied")
            operation_id = payload.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise PermissionError("invalid Portal audit operation")
            self._knowledge_operation_local.operation = {
                "operation_id": operation_id,
                "employee_name": employee_name,
                "context_handle": knowledge_context_handle,
            }
            return [kb_id for kb_id in knowledge_base_ids if kb_id in allowed_set]
        except (RequestException, ValueError) as exc:
            logger.warning("Portal knowledge authorization unavailable: %s", type(exc).__name__)
            raise PermissionError("online knowledge authorization is unavailable") from exc

    def authorize_knowledge_base(
        self, kb_id: str, employee_name: str = "", knowledge_context_handle: str = "", action: str = "runtime_read"
    ) -> str:
        # Do not resolve names by listing the tenant-wide KB catalog before
        # authorization. Runtime reads must use the explicit KB id supplied
        # by the employee binding; a name lookup would be an unscoped read.
        if not isinstance(kb_id, str) or not kb_id or len(kb_id) > 128 or "/" in kb_id or "\\" in kb_id:
            raise PermissionError("explicit knowledge base id is required")
        allowed = self.authorize_knowledge_bases(
            [kb_id], employee_name, knowledge_context_handle, require_all=True, action=action
        )
        return kb_id if allowed is None else allowed[0]

    def authorize_document(
        self, knowledge_id: str, employee_name: str = "", knowledge_context_handle: str = "", action: str = "runtime_read"
    ) -> None:
        self._knowledge_operation_local.operation = None
        if not self.portal_knowledge_auth_url:
            if PORTAL_KNOWLEDGE_AUTH_REQUIRED or employee_name or knowledge_context_handle:
                raise PermissionError("online knowledge authorization is unavailable")
            return
        if (
            not _EMPLOYEE_NAME_RE.fullmatch(employee_name)
            or not _OPAQUE_CONTEXT_RE.fullmatch(knowledge_context_handle)
            or not knowledge_id
            or len(knowledge_id) > 128
        ):
            raise PermissionError("knowledge session context and document id are required")
        endpoint = f"{self.portal_knowledge_auth_url}/{urllib.parse.quote(employee_name, safe='')}/authorize"
        try:
            response = requests.post(
                endpoint,
                json={"document_id": knowledge_id, "action": action},
                headers={"X-DeerFlow-Knowledge-Context": knowledge_context_handle},
                timeout=5,
                allow_redirects=False,
            )
            if response.status_code != 200:
                raise PermissionError("knowledge access denied")
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("knowledge_base_id"), str) or not payload["knowledge_base_id"]:
                raise PermissionError("invalid Portal authorization response")
            operation_id = payload.get("operation_id")
            if not isinstance(operation_id, str) or not operation_id:
                raise PermissionError("invalid Portal audit operation")
            self._knowledge_operation_local.operation = {
                "operation_id": operation_id,
                "employee_name": employee_name,
                "context_handle": knowledge_context_handle,
            }
        except (RequestException, ValueError) as exc:
            logger.warning("Portal document authorization unavailable: %s", type(exc).__name__)
            raise PermissionError("online knowledge authorization is unavailable") from exc

    def _complete_knowledge_operation(self, success: bool, *, required: bool) -> None:
        operation = getattr(self._knowledge_operation_local, "operation", None)
        self._knowledge_operation_local.operation = None
        if operation is None:
            if required and PORTAL_KNOWLEDGE_AUTH_REQUIRED:
                raise PermissionError("knowledge audit operation is unavailable")
            return
        endpoint = f"{self.portal_knowledge_auth_url}/{urllib.parse.quote(operation['employee_name'], safe='')}/authorize"
        try:
            response = requests.post(
                endpoint,
                json={"operation_id": operation["operation_id"], "outcome": "succeeded" if success else "failed", "status_code": 200 if success else 502},
                headers={"X-DeerFlow-Knowledge-Context": operation["context_handle"]},
                timeout=5,
                allow_redirects=False,
            )
            if response.status_code != 204:
                raise PermissionError("knowledge audit completion was rejected")
        except (RequestException, ValueError) as exc:
            logger.warning("Portal knowledge audit completion unavailable: %s", type(exc).__name__)
            if required:
                raise PermissionError("knowledge audit completion is unavailable") from exc

    @contextmanager
    def authorized_read(self):
        """Complete Portal audit only after the actual upstream read returns."""
        try:
            yield
        except Exception:
            try:
                self._complete_knowledge_operation(False, required=PORTAL_KNOWLEDGE_AUTH_REQUIRED)
            except Exception as audit_error:
                logger.warning("Portal knowledge failure audit unavailable: %s", type(audit_error).__name__)
                if PORTAL_KNOWLEDGE_AUTH_REQUIRED:
                    raise PermissionError("knowledge audit failure completion is unavailable") from audit_error
            raise
        else:
            self._complete_knowledge_operation(True, required=PORTAL_KNOWLEDGE_AUTH_REQUIRED)

    def _new_session(self) -> requests.Session:
        session = requests.Session()
        session.verify = self.verify_ssl
        session.headers.update(
            {
                "X-API-Key": self.api_key,
                "Content-Type": "application/json",
            }
        )
        return session

    @property
    def session(self) -> requests.Session:
        if not getattr(self._session_local, "session", None):
            self._session_local.session = self._new_session()
        return self._session_local.session

    def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make a request to the WeKnora API

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path
            **kwargs: Additional arguments to pass to requests

        Returns:
            JSON response as dictionary
        """
        url = f"{self.base_url}{endpoint}"
        try:
            # Execute HTTP request with the specified method
            response = self.session.request(method, url, **kwargs)
            # Raise exception for HTTP error status codes (4xx, 5xx)
            response.raise_for_status()
            # Parse and return JSON response
            return response.json()
        except RequestException as e:
            logger.error(f"API request failed: {e}")
            raise

    # Tenant Management - Methods for managing multi-tenant configurations
    def create_tenant(
        self, name: str, description: str, business: str, retriever_engines: Dict
    ) -> Dict:
        """Create a new tenant with specified configuration"""
        data = {
            "name": name,
            "description": description,
            "business": business,
            "retriever_engines": retriever_engines,  # Configuration for search engines
        }
        return self._request("POST", "/tenants", json=data)

    def get_tenant(self, tenant_id: str) -> Dict:
        """Get tenant information"""
        return self._request("GET", f"/tenants/{tenant_id}")

    def list_tenants(self) -> Dict:
        """List all tenants"""
        return self._request("GET", "/tenants")

    # Knowledge Base Management - Methods for managing knowledge bases
    def create_knowledge_base(self, name: str, description: str, config: Dict) -> Dict:
        """Create a new knowledge base with chunking and model configuration"""
        data = {
            "name": name,
            "description": description,
            **config,  # Merge additional configuration (chunking, models, etc.)
        }
        return self._request("POST", "/knowledge-bases", json=data)

    def list_knowledge_bases(self) -> Dict:
        """List all knowledge bases"""
        return self._request("GET", "/knowledge-bases")

    def list_shared_knowledge_bases(self) -> Dict:
        """List knowledge bases shared from other workspaces"""
        return self._request("GET", "/shared-knowledge-bases")

    def get_knowledge_base(self, kb_id: str) -> Dict:
        """Get knowledge base details"""
        return self._request("GET", f"/knowledge-bases/{kb_id}")

    def update_knowledge_base(self, kb_id: str, updates: Dict) -> Dict:
        """Update knowledge base"""
        return self._request("PUT", f"/knowledge-bases/{kb_id}", json=updates)

    def delete_knowledge_base(self, kb_id: str) -> Dict:
        """Delete knowledge base"""
        return self._request("DELETE", f"/knowledge-bases/{kb_id}")

    # ── UUID pattern (8-4-4-4-12 hex) ──────────────────────────────────────
    _UUID_RE = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    )

    def resolve_agent_id(self, agent_id_or_name: str) -> str:
        """Resolve an agent ID or name to its canonical ID.

        If *agent_id_or_name* is already a UUID it is returned unchanged.
        Otherwise all agents are listed and the first one whose ``id``
        matches exactly or whose ``name`` matches case-insensitively is
        returned.
        Raises ValueError when no match is found.
        """
        if self._UUID_RE.match(agent_id_or_name):
            return agent_id_or_name
        resp = self._request("GET", "/agents")
        agents = resp.get("data", resp) if isinstance(resp, dict) else resp
        if isinstance(agents, dict):
            agents = agents.get("list", agents.get("items", []))
        needle = agent_id_or_name.lower()
        for agent in (agents or []):
            if not isinstance(agent, dict):
                continue
            if agent.get("id") == agent_id_or_name:
                return agent["id"]
            if agent.get("name", "").lower() == needle:
                return agent["id"]
        raise ValueError(
            f"Agent {agent_id_or_name!r} not found. "
            "Use list_agents to see available agent IDs and names."
        )

    def resolve_kb_id(self, kb_id_or_name: str) -> str:
        """Resolve a knowledge base name to its UUID if needed.

        If *kb_id_or_name* is already a UUID it is returned unchanged.
        Otherwise all knowledge bases are listed and the first one whose
        ``name`` matches (case-insensitive) is returned.
        Raises ValueError when no match is found.
        """
        if self._UUID_RE.match(kb_id_or_name):
            return kb_id_or_name
        # Search own + shared knowledge bases for a name match
        needle = kb_id_or_name.lower()
        for source in (self.list_knowledge_bases, self.list_shared_knowledge_bases):
            for kb in _normalize_kb_entries(source()):
                if kb.get("name", "").lower() == needle:
                    return kb["id"]
        raise ValueError(
            f"Knowledge base {kb_id_or_name!r} not found. "
            "Use list_knowledge_bases or list_shared_knowledge_bases to see available IDs and names."
        )

    def hybrid_search(self, kb_id: str, query: str, config: Dict) -> Dict:
        """Perform hybrid search combining vector and keyword search"""
        data = {
            "query_text": query,
            **config,  # Include thresholds and match count
        }
        return self._request(
            "POST", f"/knowledge-bases/{kb_id}/hybrid-search", json=data
        )

    # Knowledge Management - Methods for creating and managing knowledge entries
    def create_knowledge_from_file(
        self,
        kb_id: str,
        file_path: str,
        enable_multimodel: bool = True,
        file_name: str = "",
    ) -> Dict:
        """Create knowledge from a local file with optional multimodal processing.

        ``file_name`` may be a path-qualified name (``docs/spec/design.pdf``);
        the backend splits it into ``folder_path`` + display name.
        """
        safe_path = resolve_upload_file_path(file_path)
        with open(safe_path, "rb") as f:
            files = {"file": f}
            data = {"enable_multimodel": str(enable_multimodel).lower()}
            if file_name:
                data["fileName"] = file_name
            # Temporarily remove Content-Type header for multipart/form-data request
            # (requests will set it automatically with boundary)
            headers = self.session.headers.copy()
            del headers["Content-Type"]
            # Use requests.post directly instead of session to avoid header conflicts
            response = requests.post(
                f"{self.base_url}/knowledge-bases/{kb_id}/knowledge/file",
                headers=headers,
                files=files,
                data=data,
                verify=self.verify_ssl,
            )
            response.raise_for_status()
            return response.json()

    def create_knowledge_from_url(
        self, kb_id: str, url: str, enable_multimodel: bool = True
    ) -> Dict:
        """Create knowledge from a web URL with optional multimodal processing"""
        data = {
            "url": url,  # Web URL to fetch and process
            "enable_multimodel": enable_multimodel,  # Enable image/multimodal extraction
        }
        return self._request(
            "POST", f"/knowledge-bases/{kb_id}/knowledge/url", json=data
        )

    def create_knowledge_from_text(
        self,
        kb_id: str,
        title: str,
        content: str,
        tag_ids: list[str] | None = None,
        status: str = "publish",
    ) -> Dict:
        """Create a knowledge entry from raw Markdown text (manual knowledge).

        ``status`` defaults to ``"publish"`` so the entry is chunked, embedded
        and made searchable immediately, which suits API/MCP callers that have
        no UI to publish drafts. Pass ``"draft"`` to save without indexing.
        """
        data = {
            "title": title,
            "content": content,
            "status": status,
        }
        if tag_ids:
            data["tag_ids"] = tag_ids
        return self._request(
            "POST", f"/knowledge-bases/{kb_id}/knowledge/manual", json=data
        )

    def update_knowledge_from_text(
        self,
        knowledge_id: str,
        content: str,
        title: str = "",
        status: str = "publish",
    ) -> Dict:
        """Update an existing manual Markdown knowledge entry.

        An empty ``title`` keeps the current title. ``status`` defaults to
        ``"publish"`` so the updated content is re-indexed immediately; pass
        ``"draft"`` to save it without indexing.
        """
        data = {
            "title": title,
            "content": content,
            "status": status,
        }
        return self._request("PUT", f"/knowledge/manual/{knowledge_id}", json=data)

    def list_knowledge(
        self,
        kb_id: str,
        page: int = 1,
        page_size: int = 20,
        folder_path: str | None = None,
        folder_scope: str = "",
    ) -> Dict:
        """List knowledge in a knowledge base.

        ``folder_path`` filters to one folder (empty string = root).
        ``folder_scope`` is passed through when the backend supports
        scoped folder listing.
        """
        params = {"page": page, "page_size": page_size}
        if folder_path is not None:
            params["folder_path"] = folder_path
        if folder_scope:
            params["folder_scope"] = folder_scope
        return self._request(
            "GET", f"/knowledge-bases/{kb_id}/knowledge", params=params
        )

    def get_knowledge(self, knowledge_id: str) -> Dict:
        """Get knowledge details"""
        return self._request("GET", f"/knowledge/{knowledge_id}")

    def delete_knowledge(self, knowledge_id: str) -> Dict:
        """Delete knowledge"""
        return self._request("DELETE", f"/knowledge/{knowledge_id}")

    # Model Management - Methods for managing AI models (LLM, Embedding, Rerank)
    def create_model(
        self,
        name: str,
        model_type: str,
        source: str,
        description: str,
        parameters: Dict,
        is_default: bool = False,
    ) -> Dict:
        """Create a new AI model configuration"""
        data = {
            "name": name,
            "type": model_type,  # KnowledgeQA, Embedding, or Rerank
            "source": source,  # local, openai, etc.
            "description": description,
            "parameters": parameters,  # API keys, base URLs, etc.
            "is_default": is_default,  # Set as default model for this type
        }
        return self._request("POST", "/models", json=data)

    def list_models(self) -> Dict:
        """List all models"""
        return self._request("GET", "/models")

    def get_model(self, model_id: str) -> Dict:
        """Get model details"""
        return self._request("GET", f"/models/{model_id}")

    # Session Management - Methods for managing chat sessions
    def create_session(
        self,
        kb_id: str,
        max_rounds: int = 5,
        enable_rewrite: bool = True,
        fallback_response: str = "Sorry, I cannot answer this question.",
        summary_model_id: str = "",
        title: str = "",
        description: str = "",
    ) -> Dict:
        """Create a new chat session with strategy configuration"""
        strategy = {
            "max_rounds": max_rounds,
            "enable_rewrite": enable_rewrite,
            "fallback_strategy": "FIXED_RESPONSE",
            "fallback_response": fallback_response,
            "embedding_top_k": 10,
            "keyword_threshold": 0.5,
            "vector_threshold": 0.7,
            "summary_model_id": summary_model_id,
        }
        data = {
            "knowledge_base_id": kb_id,
            "session_strategy": strategy,
        }
        if title:
            data["title"] = title
        if description:
            data["description"] = description
        return self._request("POST", "/sessions", json=data)

    def get_session(self, session_id: str) -> Dict:
        """Get session details"""
        return self._request("GET", f"/sessions/{session_id}")

    def list_sessions(self, page: int = 1, page_size: int = 20) -> Dict:
        """List sessions"""
        params = {"page": page, "page_size": page_size}
        return self._request("GET", "/sessions", params=params)

    def delete_session(self, session_id: str) -> Dict:
        """Delete session"""
        return self._request("DELETE", f"/sessions/{session_id}")

    # Chat Functionality - Methods for conversational interactions
    def _consume_sse_stream(self, url: str, body: Dict[str, Any]) -> Dict:
        """POST to *url* with *body*, consume the SSE stream, and return the assembled result.

        Centralised helper used by both chat() and agent_chat().
        Timeout: (10s connect, WEKNORA_CHAT_TIMEOUT read) — configurable via env var.
        
        Server-Sent Events (SSE) stream format:
          data: {"response_type": "answer", "content": "..."}
          data: {"response_type": "references", "knowledge_references": [...]}
          data: {"response_type": "complete"}
        
        We accumulate answer chunks and extract references, returning them as a dict.
        """
        try:
            # POST with stream=True to receive server-sent events incrementally
            # Timeout: 10s to establish connection, WEKNORA_CHAT_TIMEOUT for reading response
            response = self.session.post(
                url, json=body, stream=True,
                timeout=(10, WEKNORA_CHAT_TIMEOUT),
            )
            response.raise_for_status()

            answer_chunks: list = []
            references: list = []
            debug_events: list = []

            # Use context manager to ensure the connection is returned to the pool
            # even when breaking early on a 'complete' event.
            with response:
                for raw_line in response.iter_lines():
                    if not raw_line:
                        continue
                    if isinstance(raw_line, bytes):
                        raw_line = raw_line.decode("utf-8")
                    # Each SSE event is prefixed with "data: " followed by JSON payload
                    if not raw_line.startswith("data:"):
                        continue
                    payload = raw_line[5:].lstrip(" ")
                    try:
                        event_data = json.loads(payload)
                    except json.JSONDecodeError:
                        continue

                    response_type = event_data.get("response_type", "")
                    debug_events.append({"type": response_type, "content": event_data.get("content", "")[:80]})

                    # Parse different SSE event types: answer chunks, references, errors, completion
                    if response_type == "answer":
                        chunk = event_data.get("content", "")
                        if chunk:
                            answer_chunks.append(chunk)
                    elif response_type == "references":
                        references = event_data.get("knowledge_references") or []
                    elif response_type == "error":
                        raise RequestException(
                            f"Server error: {event_data.get('content', 'unknown error')}"
                        )
                    elif response_type == "complete":
                        break

            return {
                "answer": "".join(answer_chunks),
                "references": references,
                "_debug_events": debug_events,
            }
        except RequestException as e:
            logger.error(f"SSE stream request failed ({url}): {e}")
            raise

    def chat(
        self,
        session_id: str,
        query: str,
        knowledge_base_ids: list = None,
        web_search_enabled: bool = False,
    ) -> Dict:
        """Send a message to the RAG pipeline (knowledge-chat) and return the assembled answer.

        Provide *knowledge_base_ids* (UUID or name) so the backend can retrieve
        relevant chunks before summarising with the LLM.
        For agentic tool-calling use agent_chat() instead.
        """
        url = f"{self.base_url}/knowledge-chat/{session_id}"
        body: Dict[str, Any] = {"query": query, "channel": "api"}
        if knowledge_base_ids:
            body["knowledge_base_ids"] = knowledge_base_ids
        if web_search_enabled:
            body["web_search_enabled"] = True
        result = self._consume_sse_stream(url, body)
        result["session_id"] = session_id
        return result

    def agent_chat(
        self,
        session_id: str,
        query: str,
        agent_id: str,
        knowledge_base_ids: list = None,
        web_search_enabled: bool = False,
    ) -> Dict:
        """Send a message to the agentic pipeline (agent-chat) and return the assembled answer.

        *agent_id* is required — the backend uses the CustomAgent config for
        tool selection (knowledge_search, web_search, SQL, etc.).
        The agent autonomously decides which knowledge bases to query;
        pass *knowledge_base_ids* to override or supplement the agent's default KBs.
        """
        url = f"{self.base_url}/agent-chat/{session_id}"
        body: Dict[str, Any] = {"query": query, "agent_id": agent_id, "channel": "api"}
        if knowledge_base_ids:
            body["knowledge_base_ids"] = knowledge_base_ids
        if web_search_enabled:
            body["web_search_enabled"] = True
        result = self._consume_sse_stream(url, body)
        result["session_id"] = session_id
        return result

    def list_agents(self, page: int = 1, page_size: int = 50) -> Dict:
        """List all custom agents available to the current tenant."""
        return self._request("GET", "/agents", params={"page": page, "page_size": page_size})

    def get_agent(self, agent_id: str) -> Dict:
        """Get full config of a single agent by UUID."""
        return self._request("GET", f"/agents/{agent_id}")

    # Chunk Management - Methods for managing knowledge chunks (text segments)
    def list_chunks(
        self, knowledge_id: str, page: int = 1, page_size: int = 20
    ) -> Dict:
        """List text chunks of a knowledge entry with pagination"""
        params = {"page": page, "page_size": page_size}
        return self._request("GET", f"/chunks/{knowledge_id}", params=params)

    def delete_chunk(self, knowledge_id: str, chunk_id: str) -> Dict:
        """Delete a chunk"""
        return self._request("DELETE", f"/chunks/{knowledge_id}/{chunk_id}")

    # Wiki Read-Only - Methods for querying LLM-generated wiki pages
    def wiki_search(self, kb_id: str, query: str, limit: int = 10) -> Dict:
        """Search wiki pages by full-text query"""
        return self._request(
            "GET",
            f"/knowledgebase/{kb_id}/wiki/search",
            params={"q": query, "limit": limit},
        )

    def wiki_read_page(self, kb_id: str, slug: str) -> Dict:
        """Read a wiki page by slug, returns full markdown + metadata + links"""
        return self._request("GET", f"/knowledgebase/{kb_id}/wiki/pages/{slug}")

    def wiki_index_view(self, kb_id: str, limit: int = 50) -> Dict:
        """Get structured wiki index with per-type directory groups"""
        return self._request(
            "GET",
            f"/knowledgebase/{kb_id}/wiki/index",
            params={"limit": limit},
        )


# Initialize MCP server instance (mcp 2.x high-level API).
# MCPServer (formerly FastMCP) builds input schemas from function type hints
# and serializes plain return values automatically.
mcp = MCPServer("weknora-server", version="1.1.1")
# Initialize WeKnora API client with configuration
client = WeKnoraClient(WEKNORA_BASE_URL, WEKNORA_API_KEY)


# ---------------------------------------------------------------------------
# Tool registrations
#
# Each tool is a plain function decorated with @mcp.tool(). Parameters are
# declared via type hints (the framework derives the JSON Schema); required
# parameters have no default. Descriptions come from the docstring. Tools
# return dicts/str and the framework handles serialization and error wrapping.
# Blocking network I/O (chat / agent_chat) is offloaded to a thread executor
# so the async event loop is not blocked.
# ---------------------------------------------------------------------------


@mcp.tool()
def create_tenant(
    name: str,
    description: str,
    business: str,
    retriever_engines: dict | None = None,
) -> dict:
    """Create a new tenant in WeKnora."""
    engines = retriever_engines or {
        "engines": [
            {"retriever_type": "keywords", "retriever_engine_type": "postgres"},
            {"retriever_type": "vector", "retriever_engine_type": "postgres"},
        ]
    }
    return client.create_tenant(name, description, business, engines)


@mcp.tool()
def list_tenants() -> dict:
    """List all tenants."""
    return client.list_tenants()


@mcp.tool()
def create_knowledge_base(
    name: str,
    description: str,
    embedding_model_id: str = "",
    summary_model_id: str = "",
) -> dict:
    """Create a new knowledge base."""
    config = {
        "chunking_config": {
            "chunk_size": 1000,
            "chunk_overlap": 200,
            "separators": ["."],
            "enable_multimodal": True,
        },
        "embedding_model_id": embedding_model_id,
        "summary_model_id": summary_model_id,
    }
    return client.create_knowledge_base(name, description, config)


@mcp.tool()
def list_knowledge_bases(
    knowledge_base_ids: list[str] | None = None,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """List all knowledge bases in the current workspace."""
    requested = knowledge_base_ids or []
    allowed = client.authorize_knowledge_bases(
        requested, knowledge_employee_name, knowledge_context_handle, require_all=False,
        action="list_bases",
    )
    with client.authorized_read():
        response = client.list_knowledge_bases()
        if allowed is None:
            return response
        allowed_set = set(allowed)
        return {"data": [kb for kb in _normalize_kb_entries(response) if kb.get("id") in allowed_set], "total": len(allowed_set)}


@mcp.tool()
def list_shared_knowledge_bases() -> dict:
    """List knowledge bases shared from other workspaces."""
    return client.list_shared_knowledge_bases()


@mcp.tool()
def get_knowledge_base(
    kb_id: str,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """Get knowledge base details."""
    allowed_id = client.authorize_knowledge_base(kb_id, knowledge_employee_name, knowledge_context_handle, "read_base")
    with client.authorized_read():
        return client.get_knowledge_base(allowed_id)


@mcp.tool()
def delete_knowledge_base(kb_id: str) -> dict:
    """Delete a knowledge base."""
    return client.delete_knowledge_base(kb_id)


@mcp.tool()
def hybrid_search(
    kb_id: str,
    query: str,
    vector_threshold: float = 0.5,
    keyword_threshold: float = 0.3,
    match_count: int = 5,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """Perform hybrid (vector + keyword) search in a knowledge base.

    Supply the explicit knowledge-base ID assigned to this employee. The ID
    must be in the authorized ``list_knowledge_bases`` scope; names and shared
    workspace catalogs are not accepted by the enterprise runtime path.
    """
    config = {
        "vector_threshold": vector_threshold,
        "keyword_threshold": keyword_threshold,
        "match_count": match_count,
    }
    _validate_hybrid_search(query, vector_threshold, keyword_threshold, match_count)
    resolved = client.authorize_knowledge_base(kb_id, knowledge_employee_name, knowledge_context_handle, "search")
    with client.authorized_read():
        return client.hybrid_search(resolved, query, config)


@mcp.tool()
def create_knowledge_from_file(
    kb_id: str,
    file_path: str,
    enable_multimodel: bool = True,
    file_name: str = "",
) -> dict:
    """Create knowledge from a local file on the server filesystem.

    ``file_name`` is optional. Pass a path-qualified name such as
    ``docs/spec/design.pdf`` to place the entry under a knowledge folder
    (backend splits folder path + display name). Omit it to keep the
    original filename at the knowledge-base root.
    """
    return client.create_knowledge_from_file(
        kb_id, file_path, enable_multimodel, file_name=file_name
    )


@mcp.tool()
def create_knowledge_from_url(
    kb_id: str,
    url: str,
    enable_multimodel: bool = True,
) -> dict:
    """Create knowledge from a web URL."""
    return client.create_knowledge_from_url(kb_id, url, enable_multimodel)


@mcp.tool()
def create_knowledge_from_text(
    kb_id: str,
    title: str,
    content: str,
    tag_ids: list[str] | None = None,
    status: str = "publish",
) -> dict:
    """Create a knowledge entry from raw Markdown text.

    Use this when you have the document content directly (e.g. an abstract or
    pasted text) instead of a file path or URL. ``kb_id`` may be a UUID or a
    knowledge-base name (resolved automatically). ``title`` and ``content``
    are required. ``status`` defaults to ``"publish"`` so the entry is indexed
    and searchable immediately; pass ``"draft"`` to save without indexing.
    """
    return client.create_knowledge_from_text(
        client.resolve_kb_id(kb_id), title, content, tag_ids=tag_ids, status=status
    )


@mcp.tool()
def update_knowledge_from_text(
    knowledge_id: str,
    content: str,
    title: str = "",
    status: str = "publish",
) -> dict:
    """Update an existing manual Markdown knowledge entry.

    ``knowledge_id`` is the ID returned by ``create_knowledge_from_text`` or
    ``list_knowledge``. ``content`` is required. Leave ``title`` empty to keep
    the current title. ``status`` defaults to ``"publish"`` so the new content
    is re-indexed; pass ``"draft"`` to save without indexing.
    """
    return client.update_knowledge_from_text(
        knowledge_id, content, title=title, status=status
    )


@mcp.tool()
def list_knowledge(
    kb_id: str,
    page: int = 1,
    page_size: int = 20,
    folder_path: str | None = None,
    folder_scope: str = "",
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """List knowledge entries in a knowledge base.

    ``folder_path`` optionally filters to one folder (``""`` = root).
    ``folder_scope`` is forwarded when the backend supports scoped listing.
    """
    _validate_read_pagination(page, page_size)
    allowed_id = client.authorize_knowledge_base(kb_id, knowledge_employee_name, knowledge_context_handle, "list_documents")
    with client.authorized_read():
        return client.list_knowledge(
            allowed_id, page, page_size, folder_path=folder_path, folder_scope=folder_scope
        )


@mcp.tool()
def get_knowledge(
    knowledge_id: str,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """Get knowledge details."""
    client.authorize_document(knowledge_id, knowledge_employee_name, knowledge_context_handle, "read_document")
    with client.authorized_read():
        return client.get_knowledge(knowledge_id)


@mcp.tool()
def delete_knowledge(knowledge_id: str) -> dict:
    """Delete a knowledge entry."""
    return client.delete_knowledge(knowledge_id)


@mcp.tool()
def create_model(
    name: str,
    type: str,
    description: str,
    source: str = "local",
    base_url: str = "",
    api_key: str = "",
    is_default: bool = False,
) -> dict:
    """Create a new model configuration (type: KnowledgeQA, Embedding, or Rerank)."""
    parameters = {"base_url": base_url, "api_key": api_key}
    return client.create_model(name, type, source, description, parameters, is_default)


@mcp.tool()
def list_models() -> dict:
    """List all models."""
    return client.list_models()


@mcp.tool()
def get_model(model_id: str) -> dict:
    """Get model details."""
    return client.get_model(model_id)


@mcp.tool()
def create_session(
    kb_id: str,
    max_rounds: int = 5,
    enable_rewrite: bool = True,
    fallback_response: str = "Sorry, I cannot answer this question.",
    summary_model_id: str = "",
    title: str = "",
    description: str = "",
) -> dict:
    """Create a new chat session bound to a knowledge base with a retrieval strategy.

    kb_id may be a UUID or a knowledge-base name (resolved automatically).
    """
    return client.create_session(
        kb_id=client.resolve_kb_id(kb_id),
        max_rounds=max_rounds,
        enable_rewrite=enable_rewrite,
        fallback_response=fallback_response,
        summary_model_id=summary_model_id,
        title=title,
        description=description,
    )


@mcp.tool()
def get_session(session_id: str) -> dict:
    """Get session details."""
    return client.get_session(session_id)


@mcp.tool()
def list_sessions(page: int = 1, page_size: int = 20) -> dict:
    """List chat sessions."""
    return client.list_sessions(page, page_size)


@mcp.tool()
def delete_session(session_id: str) -> dict:
    """Delete a session."""
    return client.delete_session(session_id)


@mcp.tool()
async def chat(
    session_id: str,
    query: str,
    knowledge_base_ids: list[str] | None = None,
    web_search_enabled: bool = False,
) -> dict:
    """RAG pipeline chat: retrieve relevant chunks from knowledge bases, then summarise with LLM.

    ALWAYS provide knowledge_base_ids (names like 'my-knowledge-base' or UUIDs) so
    retrieval can run — without them the answer is based on LLM knowledge only.
    Use list_knowledge_bases or list_shared_knowledge_bases to discover available knowledge bases.
    For multi-step reasoning or tool-calling use agent_chat instead.
    """
    kb_ids = (
        [client.resolve_kb_id(k) for k in knowledge_base_ids]
        if knowledge_base_ids
        else None
    )
    fn = functools.partial(
        client.chat,
        session_id,
        query,
        knowledge_base_ids=kb_ids,
        web_search_enabled=web_search_enabled,
    )
    # get_running_loop() is the correct API inside async functions.
    return await asyncio.get_running_loop().run_in_executor(None, fn)


@mcp.tool()
async def agent_chat(
    session_id: str,
    query: str,
    agent_id: str,
    knowledge_base_ids: list[str] | None = None,
    web_search_enabled: bool = False,
) -> dict:
    """Agentic pipeline chat: the agent autonomously calls tools (knowledge_search, web_search, SQL, etc.).

    REQUIRED: agent_id (name or UUID) — use list_agents to discover agents.
    IMPORTANT: many agents have KBSelectionMode=none and NO built-in knowledge bases.
    In that case you MUST pass knowledge_base_ids, otherwise the agent will fail
    with 'no search targets available'. Use get_agent to inspect an agent's
    kb_selection_mode and knowledge_bases before calling. If kb_selection_mode is
    'none' or 'selected' with an empty list, always provide knowledge_base_ids.
    """
    resolved_agent_id = client.resolve_agent_id(agent_id)
    kb_ids = (
        [client.resolve_kb_id(k) for k in knowledge_base_ids]
        if knowledge_base_ids
        else None
    )
    # Pre-check: if no KB IDs provided, inspect agent config to detect
    # kb_selection_mode=none/selected-empty so we fail fast with a clear message
    # instead of the cryptic backend error "no search targets available".
    if not kb_ids:
        try:
            agent_info = client.get_agent(resolved_agent_id)
            cfg = (agent_info.get("data") or agent_info).get("config") or {}
            mode = cfg.get("kb_selection_mode", "selected")
            built_in_kbs = cfg.get("knowledge_bases") or []
            needs_kbs = (mode == "none") or (
                mode in ("selected", "") and not built_in_kbs
            )
            if needs_kbs:
                all_kbs = _normalize_kb_entries(
                    client.list_knowledge_bases()
                ) + _normalize_kb_entries(client.list_shared_knowledge_bases())
                seen_ids: set[str] = set()
                unique_kbs: list[Dict] = []
                for kb in all_kbs:
                    kb_id = kb.get("id")
                    if kb_id and kb_id not in seen_ids:
                        seen_ids.add(kb_id)
                        unique_kbs.append(kb)
                kb_summary = ", ".join(
                    f"{kb.get('name')} ({kb.get('id')})" for kb in unique_kbs[:10]
                )
                raise ValueError(
                    f"Agent '{agent_id}' has kb_selection_mode='{mode}' with no built-in "
                    f"knowledge bases. You must provide knowledge_base_ids. "
                    f"Available knowledge bases: [{kb_summary}]"
                )
        except ValueError:
            raise
        except Exception as preflight_err:
            logger.warning(
                "agent_chat preflight KB check failed (non-fatal): %s", preflight_err
            )
    fn = functools.partial(
        client.agent_chat,
        session_id,
        query,
        resolved_agent_id,
        knowledge_base_ids=kb_ids,
        web_search_enabled=web_search_enabled,
    )
    return await asyncio.get_running_loop().run_in_executor(None, fn)


@mcp.tool()
def list_agents(page: int = 1, page_size: int = 50) -> dict:
    """List all custom agents available to the current tenant.

    Use this to discover agent IDs, names, and their KB selection mode before
    calling agent_chat.
    """
    return client.list_agents(page=page, page_size=page_size)


@mcp.tool()
def get_agent(agent_id: str) -> dict:
    """Get full configuration of a single agent by UUID or name.

    Check kb_selection_mode and knowledge_bases fields: if kb_selection_mode is
    'none' or 'selected' with an empty knowledge_bases list, you MUST pass
    knowledge_base_ids when calling agent_chat.
    """
    resolved_id = client.resolve_agent_id(agent_id)
    return client.get_agent(resolved_id)


@mcp.tool()
def list_chunks(
    knowledge_id: str,
    page: int = 1,
    page_size: int = 20,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """List chunks (text segments) of a knowledge entry."""
    _validate_read_pagination(page, page_size)
    client.authorize_document(knowledge_id, knowledge_employee_name, knowledge_context_handle, "read_chunks")
    with client.authorized_read():
        return client.list_chunks(knowledge_id, page, page_size)


@mcp.tool()
def delete_chunk(knowledge_id: str, chunk_id: str) -> dict:
    """Delete a chunk."""
    return client.delete_chunk(knowledge_id, chunk_id)


@mcp.tool()
def wiki_search(
    kb_id: str,
    query: str,
    limit: int = 10,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """Search wiki pages by full-text query.

    Returns matching wiki pages with title, slug, summary, and content snippets.
    """
    allowed_id = client.authorize_knowledge_base(kb_id, knowledge_employee_name, knowledge_context_handle, "wiki_search")
    with client.authorized_read():
        return client.wiki_search(allowed_id, query, limit)


@mcp.tool()
def wiki_read_page(
    kb_id: str,
    slug: str,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """Read a wiki page by its slug.

    Returns full markdown content, metadata, inbound/outbound links, and source
    references. slug example: 'entity/acme-corp', 'concept/rag'.
    """
    allowed_id = client.authorize_knowledge_base(kb_id, knowledge_employee_name, knowledge_context_handle, "wiki_read_page")
    with client.authorized_read():
        return client.wiki_read_page(allowed_id, slug)


@mcp.tool()
def wiki_index_view(
    kb_id: str,
    limit: int = 50,
    knowledge_employee_name: str = "",
    knowledge_context_handle: str = "",
) -> dict:
    """Get a structured wiki index with per-type directory groups.

    Returns an overview of all wiki pages organized by type (entity, concept,
    summary, etc.).
    """
    allowed_id = client.authorize_knowledge_base(kb_id, knowledge_employee_name, knowledge_context_handle, "wiki_index")
    with client.authorized_read():
        return client.wiki_index_view(allowed_id, limit)


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------


async def run_stdio():
    """Run the MCP server using stdio transport."""
    set_active_transport("stdio")
    await mcp.run_stdio_async()


async def run_sse(host: str, port: int):
    """Run the MCP server using SSE transport (legacy MCP clients)."""
    set_active_transport("sse")
    auth_token = require_network_transport_auth("sse")
    try:
        import uvicorn
    except ImportError as e:
        raise ImportError(
            f"SSE transport requires 'starlette' and 'uvicorn': pip install starlette uvicorn\n{e}"
        ) from e

    starlette_app = MCPAuthMiddleware(
        mcp.sse_app(host=host, message_path=SSE_MESSAGE_PATH),
        auth_token,
    )

    logger.info("Starting SSE MCP server on %s:%d", host, port)
    logger.info("SSE endpoint:  http://%s:%d/sse", host, port)
    logger.info("SSE messages: http://%s:%d%s", host, port, SSE_MESSAGE_PATH)
    config = uvicorn.Config(starlette_app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def run_http(host: str, port: int):
    """Run the MCP server using Streamable HTTP transport (MCP 2025-03-26 spec)."""
    set_active_transport("http")
    auth_token = require_network_transport_auth("http")
    try:
        import uvicorn
    except ImportError as e:
        raise ImportError(
            f"HTTP transport requires 'starlette' and 'uvicorn': pip install starlette uvicorn\n{e}"
        ) from e

    starlette_app = MCPAuthMiddleware(
        mcp.streamable_http_app(host=host, stateless_http=STREAMABLE_HTTP_STATELESS),
        auth_token,
    )

    logger.info("Starting Streamable HTTP MCP server on %s:%d", host, port)
    logger.info("MCP endpoint:  http://%s:%d/mcp", host, port)
    config = uvicorn.Config(starlette_app, host=host, port=port, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


# Backward-compatible alias used by run_server.py
run = run_stdio



def main():
    """Main entry point — supports stdio, sse, and http transports.

    Transport selection (in priority order):
      1. --transport CLI flag
      2. MCP_TRANSPORT environment variable
      3. Default: stdio
    """
    parser = argparse.ArgumentParser(description="WeKnora MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "http"],
        default=os.getenv("MCP_TRANSPORT", "stdio"),
        help="Transport type: stdio (default), sse, or http",
    )
    parser.add_argument(
        "--host",
        default=os.getenv("MCP_HOST", "127.0.0.1"),
        help="Bind host for network transports (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("MCP_PORT", "8000")),
        help="Bind port for network transports (default: 8000)",
    )
    args = parser.parse_args()

    if args.transport == "stdio":
        asyncio.run(run_stdio())
    elif args.transport == "sse":
        asyncio.run(run_sse(args.host, args.port))
    elif args.transport == "http":
        asyncio.run(run_http(args.host, args.port))


if __name__ == "__main__":
    main()
