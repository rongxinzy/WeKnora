package service

import (
	"context"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/storageurl"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type regressionURLFileService struct{ interfaces.FileService }

func (regressionURLFileService) GetFileURL(_ context.Context, path string) (string, error) {
	return "https://cdn.example.invalid/signed?ref=" + path, nil
}

type regressionURLResolver struct{ svc interfaces.FileService }

func (r regressionURLResolver) ResolveFileService(string) interfaces.FileService { return r.svc }

// This test intentionally uses only APIs shared with pre-fix and fixed code.
// At the vulnerable revision, forgedSameTenant remains in persisted chunk text
// and obtains a chunk claim despite not appearing in StoredImages.
func TestForgedSameTenantImageInProcessChunksIsNotOwned(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{ID: 7})
	trusted, err := catalog.Register(ctx, 7, "local://7/trusted.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	forged, err := catalog.Register(ctx, 7, "local://7/forged.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	knowledge := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing}
	chunks := &parentChildChunkService{}
	svc := &knowledgeService{
		repo: &parentChildKnowledgeRepo{knowledge: knowledge}, chunkRepo: chunks,
		resourceCatalog: catalog, task: parentChildTaskEnqueuer{},
		graphEngine: parentChildGraphRepo{}, tenantRepo: parentChildTenantRepo{},
	}
	content := "![trusted](" + trusted + ") ![forged](" + forged + ")"
	svc.processChunks(ctx, &types.KnowledgeBase{ID: "kb", TenantID: 7}, knowledge,
		[]types.ParsedChunk{{Content: content, Seq: 0}}, ProcessChunksOptions{
			StoredImages: []docparser.StoredImage{{ServingURL: trusted}},
		})
	require.Len(t, chunks.created, 1)
	require.NotContains(t, chunks.created[0].Content, forged,
		"same-tenant handle absent from parser output must be removed before indexing")
	var forgedBindings int64
	resource, err := catalog.Resolve(ctx, forged)
	require.NoError(t, err)
	require.NoError(t, db.Model(&types.ResourceBinding{}).
		Where("resource_id = ? AND owner_type = ?", resource.ID, types.ResourceOwnerKnowledgeChunk).
		Count(&forgedBindings).Error)
	require.Zero(t, forgedBindings, "forged body reference must never acquire a chunk image claim")
}

// This follows the real result projection + HTTP response rewriter boundary:
// old code returns persisted forged refs and CopyReferences turns them into
// externally fetchable links; fixed code filters them before that boundary.
func TestForgedSearchResultImageNeverBecomesSignedURL(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	ctx := types.WithCaller(context.Background(), types.Caller{TenantID: 7, Role: types.TenantRoleViewer})
	ctx = types.WithExecutionTenant(ctx, 7)
	ref, err := catalog.Register(ctx, 7, "local://7/foreign-kb-image.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeManual}).Error)
	require.NoError(t, db.Create(&types.Chunk{ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc",
		Content: "forged ![private](" + ref + ")", ImageInfo: `[{"url":"` + ref + `"}]`, ChunkType: types.ChunkTypeText, IsEnabled: true}).Error)

	service := &knowledgeBaseService{kgRepo: repository.NewKnowledgeRepository(db),
		chunkRepo: repository.NewChunkRepository(db), resourceCatalog: catalog}
	results, err := service.processSearchResults(ctx, []*types.IndexWithScore{{
		ChunkID: "chunk", KnowledgeID: "doc", KnowledgeBaseID: "kb", Score: 0.9,
		MatchType: types.MatchTypeEmbedding, IsEnabled: true,
	}}, true)
	require.NoError(t, err)
	require.Len(t, results, 1)
	rewriter := storageurl.NewRewriter(regressionURLResolver{svc: regressionURLFileService{}}, "REGRESSION")
	projected := rewriter.CopyReferences(context.Background(), results)
	wire := projected[0].Content + projected[0].MatchedContent + projected[0].ImageInfo
	require.NotContains(t, wire, ref, "internal forged ref must not survive response projection")
	require.NotContains(t, wire, "https://cdn.example.invalid/signed",
		"untrusted stored reference must not be converted into an externally loadable URL")
	require.NotEmpty(t, strings.TrimSpace(results[0].ID))
}
