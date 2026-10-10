package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/ratelimit"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"
)

func portalReadRequest() mcp.CallToolRequest {
	var req mcp.CallToolRequest
	req.Params.Name = types.MCPEndpointToolSearchKnowledge
	req.Params.Arguments = map[string]any{"knowledge_context_handle": strings.Repeat("a", 43), "knowledge_employee_name": "employee"}
	return req
}

func TestGovernedReadCompletesAuditOnlyAfterToolOutcome(t *testing.T) {
	for _, fails := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fails], func(t *testing.T) {
			var actions []map[string]any
			called := false
			callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/employee/authorize", r.URL.Path)
				require.Equal(t, strings.Repeat("a", 43), r.Header.Get("X-DeerFlow-Knowledge-Context"))
				var input map[string]any
				require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
				actions = append(actions, input)
				if input["operation_id"] != nil {
					require.True(t, called, "authorization must not be recorded as a completed read")
					want := "succeeded"
					if fails {
						want = "failed"
					}
					require.Equal(t, want, input["outcome"])
					w.WriteHeader(http.StatusNoContent)
					return
				}
				require.Equal(t, "runtime_read", input["action"])
				require.Equal(t, true, input["require_all"])
				require.Equal(t, []any{"kb"}, input["knowledge_base_ids"])
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"knowledge_base_ids": []string{"kb"}, "operation_id": "operation"}))
			}))
			defer callback.Close()
			s := &Server{portalKnowledgeAuthURL: callback.URL, portalKnowledgeAuthClient: callback.Client(), limiter: ratelimit.New(nil, "test", 0, "")}
			ctx := context.WithValue(context.Background(), types.MCPEndpointContextKey, &types.MCPEndpoint{ID: "ep", Tools: types.StringArray{types.MCPEndpointToolSearchKnowledge}, RateLimitPerMinute: 100})
			handler := s.guardTool(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				ids, err := s.portalAuthorizedKnowledgeBases(ctx, req, []string{"kb"}, true)
				require.NoError(t, err)
				require.Equal(t, []string{"kb"}, ids)
				called = true
				if fails {
					return nil, errors.New("upstream failed")
				}
				return mcp.NewToolResultText("passage"), nil
			})
			result, err := handler(ctx, portalReadRequest())
			if fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.False(t, result.IsError)
			}
			require.Len(t, actions, 2)
		})
	}
}

func TestGovernedReadRejectsUnexpectedScopeAndCompletesFailureAudit(t *testing.T) {
	completed := false
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		if input["operation_id"] != nil {
			completed = true
			require.Equal(t, "failed", input["outcome"])
			w.WriteHeader(http.StatusNoContent)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"knowledge_base_ids": []string{"foreign"}, "operation_id": "operation"}))
	}))
	defer callback.Close()
	s := &Server{portalKnowledgeAuthURL: callback.URL, portalKnowledgeAuthClient: callback.Client(), limiter: ratelimit.New(nil, "test", 0, "")}
	ctx := context.WithValue(context.Background(), types.MCPEndpointContextKey, &types.MCPEndpoint{ID: "ep", Tools: types.StringArray{types.MCPEndpointToolSearchKnowledge}, RateLimitPerMinute: 100})
	handler := s.guardTool(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_, err := s.portalAuthorizedKnowledgeBases(ctx, req, []string{"kb"}, true)
		require.Error(t, err)
		return mcp.NewToolResultError("denied"), nil
	})
	result, err := handler(ctx, portalReadRequest())
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.True(t, completed)
}

func TestGovernedReadFailsClosedWithoutCallbackAndBlocksUnsupportedTools(t *testing.T) {
	s := &Server{portalKnowledgeAuthRequired: true, limiter: ratelimit.New(nil, "test", 0, "")}
	ctx := context.WithValue(context.Background(), types.MCPEndpointContextKey, &types.MCPEndpoint{ID: "ep", Tools: types.StringArray{types.MCPEndpointToolAsk}, RateLimitPerMinute: 100})
	var req mcp.CallToolRequest
	req.Params.Name = types.MCPEndpointToolAsk
	result, err := s.guardTool(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		t.Fatal("unsupported synthesis must not bypass governed authorization")
		return nil, nil
	})(ctx, req)
	require.NoError(t, err)
	require.True(t, result.IsError)
	_, err = s.portalAuthorizedKnowledgeBases(context.Background(), portalReadRequest(), []string{"kb"}, true)
	require.Error(t, err)
}

func TestGovernedReadDoesNotReturnContentWhenAuditCompletionFails(t *testing.T) {
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var input map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&input))
		if input["operation_id"] != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"knowledge_base_ids": []string{"kb"}, "operation_id": "operation"}))
	}))
	defer callback.Close()
	s := &Server{portalKnowledgeAuthURL: callback.URL, portalKnowledgeAuthClient: callback.Client(), limiter: ratelimit.New(nil, "test", 0, "")}
	ctx := context.WithValue(context.Background(), types.MCPEndpointContextKey, &types.MCPEndpoint{ID: "ep", Tools: types.StringArray{types.MCPEndpointToolSearchKnowledge}, RateLimitPerMinute: 100})
	result, err := s.guardTool(func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		_, err := s.portalAuthorizedKnowledgeBases(ctx, req, []string{"kb"}, true)
		require.NoError(t, err)
		return mcp.NewToolResultText("private passage"), nil
	})(ctx, portalReadRequest())
	require.NoError(t, err)
	require.True(t, result.IsError)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private passage")
}

func TestGovernedReadCannotSucceedWithoutAuthorization(t *testing.T) {
	s := &Server{portalKnowledgeAuthRequired: true, limiter: ratelimit.New(nil, "test", 0, "")}
	ctx := context.WithValue(context.Background(), types.MCPEndpointContextKey, &types.MCPEndpoint{ID: "ep", Tools: types.StringArray{types.MCPEndpointToolSearchKnowledge}, RateLimitPerMinute: 100})
	result, err := s.guardTool(func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return mcp.NewToolResultText("private passage"), nil
	})(ctx, portalReadRequest())
	require.NoError(t, err)
	require.True(t, result.IsError)
}
