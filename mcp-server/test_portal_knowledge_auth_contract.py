"""Standalone Portal authorization regressions discovered by existing CI."""

import inspect
import unittest
from unittest.mock import Mock, patch

import weknora_mcp_server as server


class PortalKnowledgeAuthContractTests(unittest.TestCase):
    def setUp(self):
        self.handle = "a" * 43
        self.employee = "sales-helper"
        self.client = server.WeKnoraClient(
            "http://weknora/api/v1", "synthetic-service-credential",
            "http://portal/internal/v1/knowledge/employees",
        )

    def authorization_response(self, bases=None, operation="operation"):
        response = Mock(status_code=200)
        response.json.return_value = {
            "knowledge_base_ids": ["kb-1"] if bases is None else bases,
            "operation_id": operation,
        }
        return response

    def test_live_scope_and_audit_complete_only_after_read(self):
        completed = Mock(status_code=204)
        with patch.object(server.requests, "post", side_effect=[self.authorization_response(), completed]) as post:
            self.assertEqual(self.client.authorize_knowledge_bases(["kb-1"], self.employee, self.handle, action="search"), ["kb-1"])
            self.assertEqual(post.call_count, 1)
            with self.client.authorized_read():
                self.assertEqual(post.call_count, 1, "authorization must remain pending until the read finishes")
            self.assertEqual(post.call_count, 2)
        auth, completion = post.call_args_list
        self.assertEqual(auth.kwargs["json"], {"knowledge_base_ids": ["kb-1"], "require_all": True, "action": "search"})
        self.assertEqual(auth.kwargs["headers"], {"X-DeerFlow-Knowledge-Context": self.handle})
        self.assertFalse(auth.kwargs["allow_redirects"])
        self.assertLessEqual(auth.kwargs["timeout"], 5)
        self.assertEqual(completion.kwargs["json"], {"operation_id": "operation", "outcome": "succeeded", "status_code": 200})

    def test_failed_read_is_not_a_successful_audit(self):
        with patch.object(server.requests, "post", side_effect=[self.authorization_response(), Mock(status_code=204)]) as post:
            self.client.authorize_knowledge_bases(["kb-1"], self.employee, self.handle)
            with self.assertRaisesRegex(RuntimeError, "synthetic upstream failure"):
                with self.client.authorized_read():
                    raise RuntimeError("synthetic upstream failure")
        self.assertEqual(post.call_args.kwargs["json"], {"operation_id": "operation", "outcome": "failed", "status_code": 502})

    def test_completion_failure_does_not_return_success(self):
        with patch.object(server.requests, "post", side_effect=[self.authorization_response(), Mock(status_code=503)]):
            self.client.authorize_knowledge_bases(["kb-1"], self.employee, self.handle)
            with self.assertRaises(PermissionError):
                with self.client.authorized_read():
                    pass

    def test_foreign_partial_or_missing_audit_scope_fails_closed(self):
        for response in [self.authorization_response(["foreign"]), self.authorization_response([]), self.authorization_response(operation=""), Mock(status_code=403)]:
            with self.subTest(response=response):
                with patch.object(server.requests, "post", return_value=response):
                    with self.assertRaises(PermissionError):
                        self.client.authorize_knowledge_bases(["kb-1"], self.employee, self.handle)

    def test_missing_context_and_unavailable_authorizer_do_not_read(self):
        with patch.object(server.requests, "post") as post:
            with self.assertRaises(PermissionError):
                self.client.authorize_knowledge_bases(["kb-1"], "", "")
            post.assert_not_called()
        with patch.object(server.requests, "post", side_effect=server.RequestException("synthetic offline")):
            with self.assertRaises(PermissionError):
                self.client.authorize_knowledge_bases(["kb-1"], self.employee, self.handle)

    def test_search_denial_precedes_any_content_read(self):
        client = Mock()
        client.authorize_knowledge_base.side_effect = PermissionError("denied")
        with patch.object(server, "client", client):
            with self.assertRaises(PermissionError):
                server.hybrid_search("kb-1", "query", knowledge_employee_name=self.employee, knowledge_context_handle=self.handle)
        client.hybrid_search.assert_not_called()

    def test_model_cannot_select_an_audit_action(self):
        for tool_name in (
            "list_knowledge_bases", "get_knowledge_base", "hybrid_search",
            "list_knowledge", "get_knowledge", "list_chunks",
            "wiki_search", "wiki_read_page", "wiki_index_view",
        ):
            with self.subTest(tool=tool_name):
                self.assertNotIn("knowledge_action", inspect.signature(getattr(server, tool_name)).parameters)

    def test_required_failed_read_audit_cannot_be_silently_lost(self):
        with patch.object(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", True):
            with patch.object(server.requests, "post", side_effect=[self.authorization_response(), Mock(status_code=503)]) as post:
                self.client.authorize_knowledge_bases(["kb-1"], self.employee, self.handle)
                with self.assertRaisesRegex(PermissionError, "audit"):
                    with self.client.authorized_read():
                        raise RuntimeError("synthetic failed read")
        self.assertEqual(post.call_args.kwargs["json"]["outcome"], "failed")


if __name__ == "__main__":
    unittest.main()
