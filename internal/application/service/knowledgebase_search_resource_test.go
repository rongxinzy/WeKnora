package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestSearchResultImageLinksRequireExactExtractedMarker(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	ctx := context.Background()
	trusted, err := catalog.Register(ctx, 7, "local://7/exports/trusted.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	forged, err := catalog.Register(ctx, 7, "local://7/exports/forged.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb"}).Error)
	require.NoError(t, catalog.Bind(ctx, trusted, types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage))
	require.NoError(t, catalog.Bind(ctx, forged, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationAttachment))

	service := &knowledgeBaseService{resourceCatalog: catalog, chunkRepo: repository.NewChunkRepository(db)}
	result := &types.SearchResult{
		KnowledgeID: "doc", KnowledgeBaseID: "kb",
		Content: "trusted " + trusted + " html resource&#58;//" + forged[len(types.ResourceScheme):] +
			" percent resource%3A%2F%2F" + forged[len(types.ResourceScheme):],
		MatchedContent: "forged " + forged,
		ImageInfo:      `[{"url":"` + trusted + `"},{"url":"` + forged + `"},{"url":"https%3A%2F%2Fexample.invalid/x.png","original_url":"` + forged + `"}]`,
	}
	require.NoError(t, service.sanitizeSearchResultResourceReferences(ctx, 7, []*types.SearchResult{result}))
	require.Contains(t, result.Content, trusted)
	require.NotContains(t, result.Content, forged)
	require.NotContains(t, result.MatchedContent, forged)
	require.Contains(t, result.ImageInfo, trusted)
	require.NotContains(t, result.ImageInfo, forged)
	require.NotContains(t, result.ImageInfo, forged)
}
