"""Exercise the registered MCP dispatcher, without governance middleware."""

import unittest
from unittest.mock import Mock, patch

from mcp.server.mcpserver.exceptions import ToolError

import weknora_mcp_server as server


def required_arguments(tool):
    """Generate valid-shaped inputs so denial happens at execution, not schema validation."""
    def sample(schema):
        if "anyOf" in schema:
            schema = next(value for value in schema["anyOf"] if value.get("type") != "null")
        if "enum" in schema:
            return schema["enum"][0]
        return {"string": "synthetic", "integer": 1, "number": 0.5,
                "boolean": True, "array": [], "object": {}}[schema.get("type", "string")]

    schema = tool.input_schema
    return {name: sample(schema["properties"][name]) for name in schema.get("required", [])}


class EnterpriseToolBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_unprotected_registered_tool_denies_before_upstream(self):
        tools = await server.mcp.list_tools()
        denied = [tool for tool in tools if tool.name not in server._PORTAL_PROTECTED_READ_TOOLS]
        self.assertTrue({"delete_knowledge_base", "list_shared_knowledge_bases", "chat", "agent_chat"}.issubset({tool.name for tool in denied}))
        with patch.object(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", True), patch.object(server, "client", Mock()) as client:
            for tool in denied:
                with self.subTest(tool=tool.name):
                    with self.assertRaisesRegex(ToolError, "unavailable under required Portal"):
                        await server.mcp.call_tool(tool.name, required_arguments(tool))
                    self.assertEqual(client.mock_calls, [], "the runtime transport bearer is not an administrator credential")

    async def test_protected_tools_keep_context_schema_and_deny_missing_context(self):
        tools = {tool.name: tool for tool in await server.mcp.list_tools()}
        client = server.WeKnoraClient("http://weknora/api/v1", "synthetic", "http://portal/authorize")
        with patch.object(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", True), patch.object(server, "client", client), patch.object(server.requests, "post") as post, patch.object(client, "_request") as upstream:
            for name in server._PORTAL_PROTECTED_READ_TOOLS:
                with self.subTest(tool=name):
                    tool = tools[name]
                    self.assertIn("knowledge_context_handle", tool.input_schema["properties"])
                    self.assertIn("knowledge_employee_name", tool.input_schema["properties"])
                    self.assertNotIn("args", tool.input_schema["properties"])
                    args = required_arguments(tool)
                    with self.assertRaises(ToolError):
                        await server.mcp.call_tool(name, args)
                    post.assert_not_called()
                    upstream.assert_not_called()

    async def test_nonenterprise_tools_remain_backward_compatible(self):
        with patch.object(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", False), patch.object(server, "client", Mock()) as client:
            client.delete_knowledge_base.return_value = {"deleted": True}
            await server.mcp.call_tool("delete_knowledge_base", {"kb_id": "synthetic"})
            client.delete_knowledge_base.assert_called_once_with("synthetic")

    async def test_protected_dispatch_performs_online_gate_and_completion(self):
        client = server.WeKnoraClient("http://weknora/api/v1", "synthetic", "http://portal/authorize")
        authorized = Mock(status_code=200)
        authorized.json.return_value = {"knowledge_base_ids": ["kb-1"], "operation_id": "synthetic-operation"}
        completed = Mock(status_code=204)
        with patch.object(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", True), patch.object(server, "client", client), patch.object(server.requests, "post", side_effect=[authorized, completed]) as post, patch.object(client, "_request", return_value={"data": []}) as upstream:
            await server.mcp.call_tool("hybrid_search", {
                "kb_id": "kb-1", "query": "synthetic query",
                "knowledge_employee_name": "employee-1", "knowledge_context_handle": "a" * 43,
            })
            upstream.assert_called_once()
            self.assertEqual(post.call_count, 2)
            self.assertEqual(post.call_args_list[0].kwargs["json"]["action"], "search")
            self.assertEqual(post.call_args_list[1].kwargs["json"]["outcome"], "succeeded")

    async def test_new_tools_are_default_denied(self):
        isolated = server.MCPServer("enterprise-boundary-regression")
        upstream = Mock()
        with patch.object(server, "mcp", isolated):
            @server.enterprise_tool
            def future_unprotected_tool() -> dict:
                return upstream()
        with patch.object(server, "PORTAL_KNOWLEDGE_AUTH_REQUIRED", True):
            with self.assertRaisesRegex(ToolError, "unavailable under required Portal"):
                await isolated.call_tool("future_unprotected_tool", {})
        upstream.assert_not_called()

    def test_invalid_required_setting_fails_closed(self):
        for value in ("", "1", "yes", "tru", "flase"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                server.parse_portal_knowledge_auth_required(value)
        self.assertTrue(server.parse_portal_knowledge_auth_required(" TRUE "))
        self.assertFalse(server.parse_portal_knowledge_auth_required("false"))


if __name__ == "__main__":
    unittest.main()
