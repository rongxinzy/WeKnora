package service

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	werrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type failingImageCatalog struct {
	interfaces.ResourceCatalog
	resolveRef string
	bindOwner  string
	bindRef    string
}

func (c failingImageCatalog) Resolve(ctx context.Context, ref string) (*types.StoredResource, error) {
	if ref == c.resolveRef {
		return nil, errors.New("injected resolve failure")
	}
	return c.ResourceCatalog.Resolve(ctx, ref)
}

func (c failingImageCatalog) Bind(ctx context.Context, ref, owner, ownerID, relation string) error {
	if owner == c.bindOwner && ref == c.bindRef {
		return errors.New("injected bind failure")
	}
	return c.ResourceCatalog.Bind(ctx, ref, owner, ownerID, relation)
}

func (c failingImageCatalog) HasBinding(ctx context.Context, ref, owner, ownerID, relation string) (bool, error) {
	return c.ResourceCatalog.(interfaces.ResourceBindingLookup).HasBinding(ctx, ref, owner, ownerID, relation)
}

type imageKBServiceStub struct {
	interfaces.KnowledgeBaseService
	kb *types.KnowledgeBase
}

func (s imageKBServiceStub) GetKnowledgeBaseByID(context.Context, string) (*types.KnowledgeBase, error) {
	return s.kb, nil
}

type imageFileServiceStub struct {
	interfaces.FileService
	reads []string
}

type imageResolveErrorCatalog struct {
	interfaces.ResourceCatalog
	ref string
}

func (c imageResolveErrorCatalog) Resolve(ctx context.Context, ref string) (*types.StoredResource, error) {
	if ref == c.ref {
		return nil, errors.New("catalog temporarily unavailable")
	}
	return c.ResourceCatalog.Resolve(ctx, ref)
}

func (s *imageFileServiceStub) GetFile(_ context.Context, path string) (io.ReadCloser, error) {
	s.reads = append(s.reads, path)
	return io.NopCloser(strings.NewReader("image-bytes")), nil
}

func newChunkImageServiceFixture(t *testing.T, bind bool) (*knowledgeService, context.Context, *imageFileServiceStub, string) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.KnowledgeBase{}, &types.Chunk{}, &types.StoredResource{}, &types.ResourceBinding{}, &types.ResourceAccessGrant{}))
	catalog := NewResourceCatalog(repository.NewResourceRepository(db))
	ref, err := catalog.Register(context.Background(), 7, "local://7/exports/diagram.png", interfaces.ResourceRegistration{Kind: "image", OriginalName: "diagram.png"})
	require.NoError(t, err)
	knowledge := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusCompleted}
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	require.NoError(t, db.Create(&types.Chunk{ID: "chunk", TenantID: 7, KnowledgeBaseID: "kb", KnowledgeID: "doc", ImageInfo: `[{"url":"` + ref + `"}]`}).Error)
	if bind {
		require.NoError(t, catalog.Bind(context.Background(), ref, types.ResourceOwnerKnowledgeChunk, "chunk", types.ResourceRelationChunkImage))
	}
	files := &imageFileServiceStub{}
	svc := &knowledgeService{
		repo: repository.NewKnowledgeRepository(db), chunkRepo: repository.NewChunkRepository(db),
		kbService:       imageKBServiceStub{kb: &types.KnowledgeBase{ID: "kb", TenantID: 7}},
		resourceCatalog: catalog, fileSvc: files,
	}
	return svc, types.WithExecutionTenant(context.Background(), 7), files, ref
}

type lifecycleKnowledgeRepositoryStub struct {
	interfaces.KnowledgeRepository
	knowledge *types.Knowledge
	changed   bool
	setCalls  int
}

func (r *lifecycleKnowledgeRepositoryStub) GetKnowledgeByID(context.Context, uint64, string) (*types.Knowledge, error) {
	return r.knowledge, nil
}

func (r *lifecycleKnowledgeRepositoryStub) SetKnowledgeEnabled(_ context.Context, _ uint64, _ string, enabled bool) (bool, error) {
	r.setCalls++
	if enabled {
		r.knowledge.ManualDisabled = false
		r.knowledge.EnableStatus = "enabled"
	} else {
		r.knowledge.ManualDisabled = true
		r.knowledge.EnableStatus = "disabled"
	}
	r.changed = true
	return true, nil
}

func TestSetKnowledgeEnabledRejectsEnableBeforeCompleted(t *testing.T) {
	repo := &lifecycleKnowledgeRepositoryStub{knowledge: &types.Knowledge{
		ID: "doc", TenantID: 7, ParseStatus: types.ParseStatusProcessing, EnableStatus: "disabled",
	}}
	svc := &knowledgeService{repo: repo}
	ctx := types.WithExecutionTenant(context.Background(), 7)

	got, err := svc.SetKnowledgeEnabled(ctx, "doc", true)
	require.Nil(t, got)
	require.Error(t, err)
	_, isAppError := werrors.IsAppError(err)
	require.True(t, isAppError)
	require.Equal(t, 0, repo.setCalls, "invalid enable must not reach repository mutation")
}

func TestSetKnowledgeEnabledPersistsManualDisableWhileProcessing(t *testing.T) {
	repo := &lifecycleKnowledgeRepositoryStub{knowledge: &types.Knowledge{
		ID: "doc", TenantID: 7, ParseStatus: types.ParseStatusProcessing, EnableStatus: "enabled",
	}}
	svc := &knowledgeService{repo: repo}
	ctx := types.WithExecutionTenant(context.Background(), 7)

	got, err := svc.SetKnowledgeEnabled(ctx, "doc", false)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.ManualDisabled)
	require.Equal(t, "disabled", got.EnableStatus)
	require.Equal(t, 1, repo.setCalls)
}

func TestGetKnowledgeChunkImageRejectsDocumentBeingDeleted(t *testing.T) {
	repo := &lifecycleKnowledgeRepositoryStub{knowledge: &types.Knowledge{
		ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusDeleting,
	}}
	svc := &knowledgeService{repo: repo}
	ctx := types.WithExecutionTenant(context.Background(), 7)

	reader, _, err := svc.GetKnowledgeChunkImage(ctx, "doc", "chunk", 0)
	require.Nil(t, reader)
	require.Error(t, err)
}

func TestGetKnowledgeChunkImageServesAuthorizedBoundImage(t *testing.T) {
	svc, ctx, files, _ := newChunkImageServiceFixture(t, true)
	reader, filename, err := svc.GetKnowledgeChunkImage(ctx, "doc", "chunk", 0)
	require.NoError(t, err)
	require.Equal(t, "diagram.png", filename)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "image-bytes", string(data))
	require.Equal(t, []string{"local://7/exports/diagram.png"}, files.reads)
}

func TestGetKnowledgeChunkImageRejectsMissingBinding(t *testing.T) {
	svc, ctx, files, _ := newChunkImageServiceFixture(t, false)
	reader, filename, err := svc.GetKnowledgeChunkImage(ctx, "doc", "chunk", 0)
	require.Error(t, err)
	require.Nil(t, reader)
	require.Empty(t, filename)
	require.Empty(t, files.reads, "unbound resource must not reach storage")
}

func TestResourceImageBindingRequiresTrustedParserReference(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/image.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.Error(t, bindChunkImageResource(ctx, catalog, 7, "chunk", ref, nil))
	require.NoError(t, bindChunkImageResource(ctx, catalog, 7, "chunk", ref, map[string]struct{}{ref: {}}))
}

func TestLegacyChunkImageCleanupNeverDeletesRawProviderPath(t *testing.T) {
	fileService := &countingFileService{}
	rows := []interfaces.ChunkImageInfo{{
		ChunkID:   "chunk",
		ImageInfo: `[{"url":"s3://bucket/other-tenant-or-object.png"},{"url":"local://7/unverified.png"}]`,
	}}
	releaseChunkImageResources(context.Background(), nil, fileService, rows, []string{"doc"})
	require.Zero(t, fileService.deleteCalls, "raw provider paths do not prove ownership")
}

func TestReplacementRawContentCannotClaimOrAuthorizeImages(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/reused.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledgeChunk, "old-chunk", types.ResourceRelationChunkImage))
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationAttachment))

	fileService := &countingFileService{}
	claims, err := holdContentResourceClaims(ctx, catalog, fileService, 7, "![reused]("+ref+")")
	require.NoError(t, err)
	require.Empty(t, claims)

	// Only the dedicated parser marker is cleaned up. A same-owner attachment
	// relation is independent and must survive owner-wide Release semantics.
	releaseChunkImageResources(ctx, catalog, fileService, []interfaces.ChunkImageInfo{{
		ChunkID: "old-chunk", ImageInfo: `[{"url":"` + ref + `"}]`,
	}}, []string{"doc"})
	resource, err := catalog.Resolve(ctx, ref)
	require.NoError(t, err, "arbitrary raw references must not be deleted as extracted images")
	require.NotNil(t, resource)
	require.Zero(t, fileService.deleteCalls)

	require.Error(t, bindChunkImageResource(ctx, catalog, 7, "new-chunk", ref, nil), "body mention is not parser provenance")
}

func TestReplacementDoesNotResolveOrClaimRawContentReferences(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/reused.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "doc", types.ResourceRelationAttachment))
	fileService := &countingFileService{}
	cleanupCalled := false

	_, err = holdReplacementResourcesBeforeCleanup(ctx, imageResolveErrorCatalog{ResourceCatalog: catalog, ref: ref},
		fileService, 7, "![reused]("+ref+")", func() error {
			cleanupCalled = true
			return nil
		})
	require.NoError(t, err)
	require.True(t, cleanupCalled, "raw references are ignored, allowing ordinary cleanup")
	resource, resolveErr := catalog.Resolve(ctx, ref)
	require.NoError(t, resolveErr, "pre-existing document resource remains active")
	require.NotNil(t, resource)
	require.Zero(t, fileService.deleteCalls)
}

func TestBindChunkImageInfoPreservesLegacyMetadataWithoutGrantingIt(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/image.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)

	imageInfo := `[{"url":"https://example.invalid/remote.png"},{"url":"s3://bucket/legacy.png"},{"url":"` + ref + `"}]`
	require.NoError(t, bindTransferredChunkImageInfo(ctx, catalog, 7, "doc", "chunk-1", imageInfo))

	resource, err := catalog.Resolve(ctx, ref)
	require.NoError(t, err)
	var bindings int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).
		Where("resource_id = ? AND owner_type = ? AND owner_id = ? AND relation = ?", resource.ID,
			types.ResourceOwnerKnowledgeChunk, "chunk-1", types.ResourceRelationChunkImage).
		Count(&bindings).Error)
	require.EqualValues(t, 1, bindings, "registered resource handles still receive a binding")
	var marker int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).
		Where("resource_id = ? AND owner_type = ? AND owner_id = ? AND relation = ?", resource.ID,
			types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage).
		Count(&marker).Error)
	require.EqualValues(t, 1, marker, "transfer provenance is tracked independently from source-file claims")
}

func TestUntrustedResourceAliasesAreRemovedBeforeIndexing(t *testing.T) {
	ref := types.BuildResourcePath(strings.Repeat("a", types.ResourceHandleLength))
	for _, input := range []string{ref, strings.ToUpper(types.ResourceScheme) + ref[len(types.ResourceScheme):], strings.Replace(ref, "://", "&#58;//", 1), strings.Replace(ref, "://", "%3A%2F%2F", 1)} {
		got := removeUntrustedResourceReferences("before "+input+" after", nil)
		require.NotContains(t, got, strings.Repeat("a", types.ResourceHandleLength), "input %q", input)
	}
	trusted := removeUntrustedResourceReferences("![x]("+ref+")", map[string]struct{}{ref: {}})
	require.Contains(t, trusted, ref)
}

func TestTransferredImageBindingRollbackCleansEarlierClaimsAndPreservesExistingMarker(t *testing.T) {
	for _, fail := range []string{"resolve-second", "chunk-bind-second", "marker-bind-second"} {
		t.Run(fail, func(t *testing.T) {
			catalog, db := newResourceCatalogForTest(t)
			ctx := context.Background()
			first, err := catalog.Register(ctx, 7, "local://7/exports/first.png", interfaces.ResourceRegistration{Kind: "image"})
			require.NoError(t, err)
			second, err := catalog.Register(ctx, 7, "local://7/exports/second.png", interfaces.ResourceRegistration{Kind: "image"})
			require.NoError(t, err)
			require.NoError(t, db.AutoMigrate(&types.Knowledge{}, &types.KnowledgeBase{}, &types.Chunk{}))
			require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
			require.NoError(t, db.Create(&types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusCompleted}).Error)
			if fail == "marker-bind-second" {
				require.NoError(t, catalog.Bind(ctx, first, types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage))
			}
			wrapped := failingImageCatalog{ResourceCatalog: catalog}
			switch fail {
			case "resolve-second":
				wrapped.resolveRef = second
			case "chunk-bind-second":
				wrapped.bindOwner, wrapped.bindRef = types.ResourceOwnerKnowledgeChunk, second
			case "marker-bind-second":
				wrapped.bindOwner, wrapped.bindRef = types.ResourceOwnerKnowledgeImage, second
			}
			err = bindTransferredChunkImageInfo(ctx, wrapped, 7, "doc", "new-chunk", `[{"url":"`+first+`"},{"url":"`+second+`"}]`)
			require.Error(t, err)
			for _, ref := range []string{first, second} {
				resource, resolveErr := catalog.Resolve(ctx, ref)
				require.NoError(t, resolveErr)
				var chunkClaims, provenance int64
				require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ? AND owner_id = ?", resource.ID,
					types.ResourceOwnerKnowledgeChunk, "new-chunk").Count(&chunkClaims).Error)
				require.Zero(t, chunkClaims)
				require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ? AND owner_id = ? AND relation = ?", resource.ID,
					types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage).Count(&provenance).Error)
				want := int64(0)
				if fail == "marker-bind-second" && ref == first {
					want = 1
				}
				require.Equal(t, want, provenance, "rollback must preserve only pre-existing provenance")
			}
		})
	}
}
