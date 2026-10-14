package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type knowledgeLifecycleHandlerStub struct {
	interfaces.KnowledgeService
	knowledge *types.Knowledge
	setErr    error
	setCalls  int
}

func (s *knowledgeLifecycleHandlerStub) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return s.knowledge, nil
}

func (s *knowledgeLifecycleHandlerStub) SetKnowledgeEnabled(_ context.Context, _ string, enabled bool) (*types.Knowledge, error) {
	s.setCalls++
	if s.setErr != nil {
		return nil, s.setErr
	}
	s.knowledge.EnableStatus = "disabled"
	if enabled {
		s.knowledge.EnableStatus = "enabled"
	}
	return s.knowledge, nil
}

func (s *knowledgeLifecycleHandlerStub) GetKnowledgeChunkImage(context.Context, string, string, int) (io.ReadCloser, string, error) {
	return nil, "", errors.New("unexpected image lookup")
}

func newKnowledgeLifecycleHandlerTestContext(t *testing.T, handler func(*gin.Context), body string, path string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(middleware.ErrorHandler())
	engine.PUT("/knowledge/:id/enable-status", handler)
	engine.GET("/knowledge/:id/chunks/:chunk_id/images/:index", handler)
	method := http.MethodPut
	if strings.Contains(path, "/images/") {
		method = http.MethodGet
	}
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	ctx := types.WithCaller(req.Context(), types.Caller{TenantID: 1, UserID: "user", Role: types.TenantRoleOwner})
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	return rec
}

func lifecycleKnowledge() *types.Knowledge {
	return &types.Knowledge{ID: "doc", TenantID: 1, KnowledgeBaseID: "kb", EnableStatus: "enabled"}
}

func TestSetKnowledgeEnabledRejectsNonBooleanInput(t *testing.T) {
	service := &knowledgeLifecycleHandlerStub{knowledge: lifecycleKnowledge()}
	h := &KnowledgeHandler{kgService: service}
	rec := newKnowledgeLifecycleHandlerTestContext(t, h.SetKnowledgeEnabled, `{"enabled":"false"}`, "/knowledge/doc/enable-status")
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Zero(t, service.setCalls)
}

func TestSetKnowledgeEnabledReturnsSafeServiceError(t *testing.T) {
	service := &knowledgeLifecycleHandlerStub{knowledge: lifecycleKnowledge(), setErr: errors.New("database detail")}
	h := &KnowledgeHandler{kgService: service}
	rec := newKnowledgeLifecycleHandlerTestContext(t, h.SetKnowledgeEnabled, `{"enabled":false}`, "/knowledge/doc/enable-status")
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "database detail")
	require.Equal(t, 1, service.setCalls)
}

func TestDownloadKnowledgeChunkImageRejectsInvalidIndexBeforeLookup(t *testing.T) {
	service := &knowledgeLifecycleHandlerStub{knowledge: lifecycleKnowledge()}
	h := &KnowledgeHandler{kgService: service}
	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/knowledge/doc/chunks/chunk/images/-1", want: http.StatusBadRequest},
		{path: "/knowledge/doc/chunks/chunk/images/not-a-number", want: http.StatusBadRequest},
		{path: "/knowledge/doc/chunks/chunk/images/", want: http.StatusNotFound},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := newKnowledgeLifecycleHandlerTestContext(t, h.DownloadKnowledgeChunkImage, "", tc.path)
			require.Equal(t, tc.want, rec.Code, rec.Body.String())
		})
	}
}
