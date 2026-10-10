package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

func TestKBFileBindingRequiresLiveKnowledgeInExactKB(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/image.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	knowledge := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Type: "file"}
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(
		t,
		catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationExtractedImage),
	)
	lookup := catalog.(interfaces.KBResourceLookup)
	for _, reference := range []string{ref, "local://7/exports/image.png"} {
		ok, err := lookup.IsReferencedByKnowledgeBase(ctx, 7, "kb", reference)
		require.NoError(t, err)
		require.True(t, ok)
		ok, err = lookup.IsReferencedByKnowledgeBase(ctx, 7, "other-kb", reference)
		require.NoError(t, err)
		require.False(t, ok)
	}
	require.NoError(t, db.Delete(knowledge).Error)
	ok, err := lookup.IsReferencedByKnowledgeBase(ctx, 7, "kb", ref)
	require.NoError(t, err)
	require.False(t, ok, "a stale binding cannot authorize a deleted document")
}

func TestKBFileLegacyTextReferencesDoNotAuthorizeFiles(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	lookup := catalog.(interfaces.KBResourceLookup)
	const reference = "local://7/exports/image.png"
	chunk := &types.Chunk{
		ID:              "chunk",
		TenantID:        7,
		KnowledgeBaseID: "kb",
		Content:         "![image](" + reference + ".private)",
	}
	require.NoError(t, db.Create(chunk).Error)
	ok, err := lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, db.Model(chunk).Update("image_info", `[{"url":"`+reference+`"}]`).Error)
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok, "text is not an ownership binding")
	require.NoError(t, db.Delete(chunk).Error)
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok)
	page := &types.WikiPage{ID: "page", TenantID: 7, KnowledgeBaseID: "kb", Content: "![image](" + reference + ")"}
	require.NoError(t, db.Create(page).Error)
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 7, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok, "text is not an ownership binding")
	ok, err = lookup.IsReferencedByKnowledgeBase(context.Background(), 8, "kb", reference)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestKnowledgeChunkImageRequiresExactDocumentChunkAndTenantBinding(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/chunk-image.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Type: "file"}).Error)
	chunk := &types.Chunk{ID: "chunk", TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb", ImageInfo: `[{"url":"` + ref + `"}]`}
	require.NoError(t, db.Create(chunk).Error)
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledgeChunk, chunk.ID, types.ResourceRelationChunkImage))
	require.NoError(t, catalog.Bind(ctx, ref, "shared_test_owner", "shared", "attachment"))

	lookup := catalog.(interfaces.KnowledgeChunkImageCatalog)
	for _, tc := range []struct {
		name, kbID, knowledgeID, chunkID string
		tenantID                         uint64
		want                             bool
	}{
		{name: "exact owner", tenantID: 7, kbID: "kb", knowledgeID: "doc", chunkID: "chunk", want: true},
		{name: "cross tenant", tenantID: 8, kbID: "kb", knowledgeID: "doc", chunkID: "chunk"},
		{name: "wrong KB", tenantID: 7, kbID: "other", knowledgeID: "doc", chunkID: "chunk"},
		{name: "wrong knowledge", tenantID: 7, kbID: "kb", knowledgeID: "other", chunkID: "chunk"},
		{name: "wrong chunk", tenantID: 7, kbID: "kb", knowledgeID: "doc", chunkID: "other"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := lookup.IsKnowledgeChunkImage(ctx, tc.tenantID, tc.kbID, tc.knowledgeID, tc.chunkID, ref)
			require.NoError(t, err)
			require.Equal(t, tc.want, ok)
		})
	}

	// Reparse/delete cleanup releases the exact chunk claim but preserves a
	// shared resource claim. A legacy image_info string alone is not a grant.
	releaseChunkImageResources(ctx, catalog, nil, []interfaces.ChunkImageInfo{{
		ChunkID: "chunk", KnowledgeID: "doc", ImageInfo: chunk.ImageInfo,
	}}, []string{"doc"})
	ok, err := lookup.IsKnowledgeChunkImage(ctx, 7, "kb", "doc", "chunk", ref)
	require.NoError(t, err)
	require.False(t, ok)
	var remaining int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", func() string {
		resource, resolveErr := catalog.Resolve(ctx, ref)
		require.NoError(t, resolveErr)
		return resource.ID
	}()).Count(&remaining).Error)
	require.EqualValues(t, 1, remaining)

	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "doc").
		Update("parse_status", types.ParseStatusDeleting).Error)
	ok, err = lookup.IsKnowledgeChunkImage(ctx, 7, "kb", "doc", "chunk", ref)
	require.NoError(t, err)
	require.False(t, ok, "a document in the asynchronous deletion window cannot authorize image reads")
}

func TestExtractedImageProvenanceIsSeparateFromAttachmentAndReleasedOnReparse(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/image.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", Type: "manual", ParseStatus: types.ParseStatusCompleted}).Error)
	resource, err := catalog.Resolve(ctx, ref)
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationAttachment))
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationSourceFile))
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage))
	require.NoError(t, db.Create(&types.Chunk{ID: "chunk", TenantID: 7, KnowledgeID: "doc", KnowledgeBaseID: "kb", ImageInfo: `[{"url":"` + ref + `"}]`}).Error)

	lookup := catalog.(interfaces.ExtractedKnowledgeImageLookup)
	var markerCount int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ? AND owner_id = ? AND relation = ?", resource.ID,
		types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage).Count(&markerCount).Error)
	require.EqualValues(t, 1, markerCount)
	allowed, err := lookup.IsExtractedImageForKnowledge(ctx, 7, "kb", "doc", ref)
	require.NoError(t, err)
	require.True(t, allowed)
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "doc").Update("parse_status", types.ParseStatusFailed).Error)
	allowed, err = lookup.IsExtractedImageForKnowledge(ctx, 7, "kb", "doc", ref)
	require.NoError(t, err)
	require.False(t, allowed, "failed parse artifacts cannot authorize image reads")
	require.NoError(t, db.Model(&types.Knowledge{}).Where("id = ?", "doc").Update("parse_status", types.ParseStatusCompleted).Error)

	releaseChunkImageResources(ctx, catalog, nil, []interfaces.ChunkImageInfo{{ChunkID: "chunk", ImageInfo: `[{"url":"` + ref + `"}]`}}, []string{"doc"})
	allowed, err = lookup.IsExtractedImageForKnowledge(ctx, 7, "kb", "doc", ref)
	require.NoError(t, err)
	require.False(t, allowed, "reparse cleanup removes the extracted-image provenance")
	var remaining int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", resource.ID).Count(&remaining).Error)
	require.EqualValues(t, 2, remaining, "releasing parser provenance must preserve source-file and attachment rows")
}
