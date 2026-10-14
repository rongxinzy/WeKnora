package router

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/handler"
	"github.com/Tencent/WeKnora/internal/middleware"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type knowledgeImageRouteStub struct {
	interfaces.KnowledgeService
	knowledge  *types.Knowledge
	imageReads int
}

func (s *knowledgeImageRouteStub) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if s.knowledge != nil && s.knowledge.ID == id {
		return s.knowledge, nil
	}
	return nil, apprepo.ErrKnowledgeNotFound
}

func (s *knowledgeImageRouteStub) GetKnowledgeChunkImage(context.Context, string, string, int) (io.ReadCloser, string, error) {
	s.imageReads++
	return io.NopCloser(bytes.NewReader([]byte("image"))), "image.png", nil
}

func (s *knowledgeImageRouteStub) SetKnowledgeEnabled(_ context.Context, id string, enabled bool) (*types.Knowledge, error) {
	if s.knowledge == nil || s.knowledge.ID != id {
		return nil, apprepo.ErrKnowledgeNotFound
	}
	if enabled {
		s.knowledge.ManualDisabled = false
		s.knowledge.EnableStatus = "enabled"
	} else {
		s.knowledge.ManualDisabled = true
		s.knowledge.EnableStatus = "disabled"
	}
	return s.knowledge, nil
}

type downloadKnowledgeLookup struct {
	knowledge *types.Knowledge
}

func TestKnowledgeLifecycleRoutesEnforceWriteAndTenantBoundaries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	enabled := true
	knowledge := &types.Knowledge{ID: "doc", KnowledgeBaseID: "kb", TenantID: 2}
	guards := &rbacGuards{
		cfg:              &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &enabled}},
		knowledgeService: &downloadKnowledgeLookup{knowledge: knowledge},
		kbService:        &stubWikiKBLookup{kbs: map[string]*types.KnowledgeBase{"kb": {ID: "kb", TenantID: 2}}},
		kbShareService:   &downloadKBShareStub{permission: types.OrgRoleViewer, source: 2},
		knowledgeKBCreator: func(*gin.Context) (string, error) {
			return "", middleware.ErrResourceNotFound
		},
	}
	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(1))
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, types.TenantRoleContributor)
		ctx = context.WithValue(ctx, types.UserIDContextKey, "owner")
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Next()
	})
	imageService := &knowledgeImageRouteStub{knowledge: knowledge}
	knowledgeHandler := handler.NewKnowledgeHandler(nil, imageService, nil, nil, nil, nil, nil)
	RegisterKnowledgeRoutes(r.Group("/api/v1"), knowledgeHandler, guards)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/knowledge/doc/enable-status", bytes.NewBufferString(`{"enabled":false}`))
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, "viewer cannot change lifecycle state: %s", rec.Body.String())

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/knowledge/doc/chunks/chunk/images/0", nil)
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "tenant Viewer can read an authorized image: %s", rec.Body.String())
	require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))
	require.Equal(t, 1, imageService.imageReads)

	guards.knowledgeService = &downloadKnowledgeLookup{knowledge: &types.Knowledge{
		ID: "foreign-doc", KnowledgeBaseID: "foreign-kb", TenantID: 2,
	}}
	guards.kbService = &stubWikiKBLookup{kbs: map[string]*types.KnowledgeBase{
		"foreign-kb": {ID: "foreign-kb", TenantID: 2},
	}}
	guards.kbShareService = nil
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/knowledge/foreign-doc/chunks/chunk/images/0", nil)
	r.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, "inaccessible cross-tenant resources are hidden: %s", rec.Body.String())
	require.Equal(t, 1, imageService.imageReads, "cross-tenant request must not reach image storage")
}

func (s *downloadKnowledgeLookup) GetKnowledgeByIDOnly(_ context.Context, id string) (*types.Knowledge, error) {
	if s.knowledge != nil && s.knowledge.ID == id {
		return s.knowledge, nil
	}
	return nil, apprepo.ErrKnowledgeNotFound
}

type downloadKBShareStub struct {
	interfaces.KBShareService
	permission types.OrgMemberRole
	source     uint64
}

func (s *downloadKBShareStub) CheckTenantKBPermission(
	_ context.Context,
	_ string,
	_ uint64,
	_ types.TenantRole,
) (types.OrgMemberRole, bool, error) {
	return s.permission, true, nil
}

func (s *downloadKBShareStub) GetKBSourceTenant(_ context.Context, _ string) (uint64, error) {
	return s.source, nil
}

func newKnowledgeDownloadRouteTestEngine(
	t *testing.T,
	role types.TenantRole,
	knowledge *types.Knowledge,
	kb *types.KnowledgeBase,
	share interfaces.KBShareService,
) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)

	enabled := true
	guards := &rbacGuards{
		cfg:              &config.Config{Tenant: &config.TenantConfig{EnableRBAC: &enabled}},
		knowledgeService: &downloadKnowledgeLookup{knowledge: knowledge},
		kbService:        &stubWikiKBLookup{kbs: map[string]*types.KnowledgeBase{kb.ID: kb}},
		kbShareService:   share,
	}

	r := gin.New()
	r.Use(middleware.ErrorHandler())
	r.Use(func(c *gin.Context) {
		ctx := context.WithValue(c.Request.Context(), types.TenantIDContextKey, uint64(1))
		ctx = context.WithValue(ctx, types.TenantRoleContextKey, role)
		c.Request = c.Request.WithContext(ctx)
		c.Set(types.TenantIDContextKey.String(), uint64(1))
		c.Next()
	})
	RegisterKnowledgeRoutes(r.Group("/api/v1"), &handler.KnowledgeHandler{}, guards)
	return r
}

func TestKnowledgeDownloadRejectsTenantViewer(t *testing.T) {
	engine := newKnowledgeDownloadRouteTestEngine(
		t,
		types.TenantRoleViewer,
		&types.Knowledge{ID: "knowledge-own", KnowledgeBaseID: "kb-own", TenantID: 1},
		&types.KnowledgeBase{ID: "kb-own", TenantID: 1},
		nil,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/knowledge/knowledge-own/download", nil)
	engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}

func TestKnowledgeDownloadRejectsReadOnlySharedKB(t *testing.T) {
	engine := newKnowledgeDownloadRouteTestEngine(
		t,
		types.TenantRoleContributor,
		&types.Knowledge{ID: "knowledge-shared", KnowledgeBaseID: "kb-shared", TenantID: 2},
		&types.KnowledgeBase{ID: "kb-shared", TenantID: 2},
		&downloadKBShareStub{permission: types.OrgRoleViewer, source: 2},
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/knowledge/knowledge-shared/download", nil)
	engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}

func TestBatchKnowledgeDownloadRejectsTenantViewer(t *testing.T) {
	engine := newKnowledgeDownloadRouteTestEngine(
		t,
		types.TenantRoleViewer,
		&types.Knowledge{ID: "knowledge-own", KnowledgeBaseID: "kb-own", TenantID: 1},
		&types.KnowledgeBase{ID: "kb-own", TenantID: 1},
		nil,
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/knowledge-bases/kb-own/knowledge/batch-download",
		bytes.NewBufferString(`{"ids":["knowledge-own"]}`),
	)
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}

func TestBatchKnowledgeDownloadRejectsReadOnlySharedKB(t *testing.T) {
	engine := newKnowledgeDownloadRouteTestEngine(
		t,
		types.TenantRoleContributor,
		&types.Knowledge{ID: "knowledge-shared", KnowledgeBaseID: "kb-shared", TenantID: 2},
		&types.KnowledgeBase{ID: "kb-shared", TenantID: 2},
		&downloadKBShareStub{permission: types.OrgRoleViewer, source: 2},
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/knowledge-bases/kb-shared/knowledge/batch-download",
		bytes.NewBufferString(`{"ids":["knowledge-shared"]}`),
	)
	req.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code, "body=%s", rec.Body.String())
}
