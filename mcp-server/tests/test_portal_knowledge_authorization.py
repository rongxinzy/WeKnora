from __future__ import annotations

from contextlib import contextmanager
import inspect
from unittest.mock import Mock, patch

import pytest

import weknora_mcp_server as server

HANDLE = "A" * 42 + "_"
EMPLOYEE = "sales-helper"
KB = "12345678-1234-1234-1234-123456789abc"


def test_portal_kb_authorization_sends_only_opaque_handle_and_live_scope() -> None:
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    response = Mock(status_code=200)
    response.json.return_value = {"knowledge_base_ids": [KB], "operation_id": "op-1"}
    with patch.object(server.requests, "post", return_value=response) as post:
        assert client.authorize_knowledge_bases([KB], EMPLOYEE, HANDLE) == [KB]
    args, kwargs = post.call_args
    assert args[0] == f"http://portal/internal/v1/knowledge/employees/{EMPLOYEE}/authorize"
    assert kwargs["headers"] == {"X-DeerFlow-Knowledge-Context": HANDLE}
    assert kwargs["json"] == {"knowledge_base_ids": [KB], "require_all": True, "action": "runtime_read"}
    assert kwargs["allow_redirects"] is False
    assert kwargs["timeout"] <= 5


def test_kb_authorization_does_not_resolve_name_from_tenant_wide_catalog() -> None:
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    client.resolve_kb_id = Mock(side_effect=AssertionError("must not list tenant-wide KBs before authorization"))
    response = Mock(status_code=200)
    response.json.return_value = {"knowledge_base_ids": [KB], "operation_id": "op-1"}
    with patch.object(server.requests, "post", return_value=response) as post:
        assert client.authorize_knowledge_base(KB, EMPLOYEE, HANDLE) == KB
    assert post.call_args.kwargs["json"]["knowledge_base_ids"] == [KB]


def test_portal_kb_authorization_rejects_partial_or_unavailable_scope() -> None:
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    response = Mock(status_code=200)
    response.json.return_value = {"knowledge_base_ids": [KB]}
    with patch.object(server.requests, "post", return_value=response):
        with pytest.raises(PermissionError, match="denied"):
            client.authorize_knowledge_bases([KB, "denied-kb"], EMPLOYEE, HANDLE)
    with patch.object(server.requests, "post", side_effect=server.RequestException("offline")):
        with pytest.raises(PermissionError, match="unavailable"):
            client.authorize_knowledge_bases([KB], EMPLOYEE, HANDLE)


def test_configured_authorizer_rejects_missing_or_malformed_context() -> None:
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    with pytest.raises(PermissionError, match="required"):
        client.authorize_knowledge_bases([KB], "", "")
    with pytest.raises(PermissionError, match="required"):
        client.authorize_knowledge_bases([KB], EMPLOYEE, "short")


def test_configured_authorizer_rejects_missing_context_before_any_read() -> None:
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    with patch.object(server.requests, "post") as post:
        with pytest.raises(PermissionError, match="required"):
            client.authorize_knowledge_base(KB, EMPLOYEE, "")
    post.assert_not_called()


def test_document_authorization_precedes_get_knowledge_read() -> None:
    calls: list[str] = []

    class FakeClient:
        def authorize_document(self, knowledge_id, employee_name, knowledge_context_handle, action):
            calls.append("authorize")

        def get_knowledge(self, knowledge_id):
            calls.append("read")
            return {"data": {"id": knowledge_id}}

        @contextmanager
        def authorized_read(self):
            yield

    previous = server.client
    server.client = FakeClient()
    try:
        result = server.get_knowledge("doc-1", EMPLOYEE, HANDLE)
    finally:
        server.client = previous
    assert result == {"data": {"id": "doc-1"}}
    assert calls == ["authorize", "read"]


def test_search_authorization_precedes_retrieval() -> None:
    calls: list[str] = []

    class FakeClient:
        def authorize_knowledge_base(self, kb_id, employee_name, knowledge_context_handle, action):
            calls.append("authorize")
            return KB

        def hybrid_search(self, kb_id, query, config):
            calls.append("search")
            return {"data": []}

        @contextmanager
        def authorized_read(self):
            yield

    previous = server.client
    server.client = FakeClient()
    try:
        result = server.hybrid_search(KB, "q", knowledge_employee_name=EMPLOYEE, knowledge_context_handle=HANDLE)
    finally:
        server.client = previous
    assert result == {"data": []}
    assert calls == ["authorize", "search"]


@pytest.mark.parametrize(
    ("tool", "kwargs"),
    [
        (server.list_knowledge, {"page": 0}),
        (server.list_knowledge, {"page_size": 101}),
        (server.list_chunks, {"page": -1}),
        (server.list_chunks, {"page_size": 0}),
    ],
)
def test_source_pagination_is_validated_before_authorization_or_read(monkeypatch, tool, kwargs) -> None:
    client = Mock()
    monkeypatch.setattr(server, "client", client)
    with pytest.raises(ValueError, match="page"):
        if tool is server.list_knowledge:
            tool("kb-1", knowledge_employee_name=EMPLOYEE, knowledge_context_handle=HANDLE, **kwargs)
        else:
            tool("doc-1", knowledge_employee_name=EMPLOYEE, knowledge_context_handle=HANDLE, **kwargs)
    client.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"query": "  "},
        {"query": "q" * 4001},
        {"vector_threshold": -0.1},
        {"keyword_threshold": 1.1},
        {"match_count": 0},
        {"match_count": 101},
    ],
)
def test_hybrid_search_query_parameters_are_validated_before_authorization(monkeypatch, kwargs) -> None:
    client = Mock()
    monkeypatch.setattr(server, "client", client)
    with pytest.raises(ValueError):
        server.hybrid_search(
            KB,
            kwargs.pop("query", "valid query"),
            knowledge_employee_name=EMPLOYEE,
            knowledge_context_handle=HANDLE,
            **kwargs,
        )
    client.assert_not_called()


def test_failed_read_audit_completion_is_fail_closed_when_required(monkeypatch) -> None:
    monkeypatch.setattr(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", True)
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    response = Mock(status_code=200)
    response.json.return_value = {"knowledge_base_ids": [KB], "operation_id": "op-1"}
    with patch.object(server.requests, "post", side_effect=[response, server.RequestException("audit unavailable")]) as post:
        client.authorize_knowledge_bases([KB], EMPLOYEE, HANDLE)
        with pytest.raises(PermissionError, match="failure completion is unavailable"):
            with client.authorized_read():
                raise RuntimeError("upstream read failed")
    assert post.call_count == 2
    assert post.call_args.kwargs["json"]["outcome"] == "failed"


def test_failed_read_audit_remains_best_effort_when_not_required(monkeypatch) -> None:
    monkeypatch.setattr(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", False)
    client = server.WeKnoraClient("http://weknora/api/v1", "service-only", "http://portal/internal/v1/knowledge/employees")
    response = Mock(status_code=200)
    response.json.return_value = {"knowledge_base_ids": [KB], "operation_id": "op-1"}
    with patch.object(server.requests, "post", side_effect=[response, server.RequestException("audit unavailable")]):
        client.authorize_knowledge_bases([KB], EMPLOYEE, HANDLE)
        with pytest.raises(RuntimeError, match="upstream read failed"):
            with client.authorized_read():
                raise RuntimeError("upstream read failed")


def test_read_tool_actions_are_fixed_and_not_model_schema_parameters(monkeypatch) -> None:
    class FixedActionClient:
        def __init__(self):
            self.actions = []

        def authorize_knowledge_bases(self, ids, employee_name, handle, *, require_all, action):
            self.actions.append(action)
            return [KB]

        def authorize_knowledge_base(self, kb_id, employee_name, handle, action):
            self.actions.append(action)
            return KB

        def authorize_document(self, knowledge_id, employee_name, handle, action):
            self.actions.append(action)

        @contextmanager
        def authorized_read(self):
            yield

        def __getattr__(self, name):
            return lambda *args, **kwargs: {"data": []}

    fake = FixedActionClient()
    monkeypatch.setattr(server, "client", fake)
    common = {"knowledge_employee_name": EMPLOYEE, "knowledge_context_handle": HANDLE}
    cases = [
        (server.list_knowledge_bases, ([KB],), common, "list_bases"),
        (server.get_knowledge_base, (KB,), common, "read_base"),
        (server.hybrid_search, (KB, "q"), common, "search"),
        (server.list_knowledge, (KB,), common, "list_documents"),
        (server.get_knowledge, ("doc-1",), common, "read_document"),
        (server.list_chunks, ("doc-1",), common, "read_chunks"),
        (server.wiki_search, (KB, "q"), common, "wiki_search"),
        (server.wiki_read_page, (KB, "entity/a"), common, "wiki_read_page"),
        (server.wiki_index_view, (KB,), common, "wiki_index"),
    ]
    for tool, positional, kwargs, expected_action in cases:
        assert "knowledge_action" not in inspect.signature(tool).parameters, tool.__name__
        tool(*positional, **kwargs)
        assert fake.actions[-1] == expected_action, tool.__name__
    with pytest.raises(TypeError):
        server.hybrid_search(KB, "q", **common, knowledge_action="delete_knowledge_base")
