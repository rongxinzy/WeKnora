package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type parentChildKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	knowledge *types.Knowledge
}

func TestProcessChunksOnlyBindsResolverProducedImages(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	tenant := &types.Tenant{ID: 7}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, tenant)
	trustedRef, err := catalog.Register(ctx, 7, "local://7/exports/trusted.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	forgedRef, err := catalog.Register(ctx, 7, "local://7/exports/forged.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	knowledge := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing}
	require.NoError(t, db.Create(knowledge).Error)
	require.NoError(t, catalog.Bind(ctx, trustedRef, types.ResourceOwnerKnowledgeImage, "doc", types.ResourceRelationExtractedImage))
	require.NoError(t, catalog.Bind(ctx, trustedRef, types.ResourceOwnerKnowledgeChunk, "old-chunk", types.ResourceRelationChunkImage))

	chunkService := &parentChildChunkService{imageInfos: []interfaces.ChunkImageInfo{{
		ChunkID: "old-chunk", KnowledgeID: "doc", ImageInfo: `[{"url":"` + trustedRef + `"}]`,
	}}}
	svc := &knowledgeService{
		repo: &parentChildKnowledgeRepo{knowledge: knowledge}, chunkRepo: chunkService,
		resourceCatalog: catalog, task: parentChildTaskEnqueuer{},
		graphEngine: parentChildGraphRepo{}, tenantRepo: parentChildTenantRepo{},
	}
	content := "![forged](" + forgedRef + ") ![trusted](" + trustedRef + ")"
	svc.processChunks(ctx, &types.KnowledgeBase{ID: "kb", TenantID: 7}, knowledge,
		[]types.ParsedChunk{{Content: content, Seq: 0}}, ProcessChunksOptions{
			StoredImages: []docparser.StoredImage{{ServingURL: trustedRef}},
		})
	require.Len(t, chunkService.created, 1)
	created := chunkService.created[0]
	require.NotContains(t, created.Content, forgedRef)
	require.Contains(t, created.Content, trustedRef)
	require.Contains(t, created.ImageInfo, trustedRef)
	require.NotContains(t, created.ImageInfo, forgedRef)
	trusted, err := catalog.Resolve(ctx, trustedRef)
	require.NoError(t, err)
	forged, err := catalog.Resolve(ctx, forgedRef)
	require.NoError(t, err)
	var trustedMarkers, forgedMarkers, chunkClaims int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ? AND relation = ?", trusted.ID,
		types.ResourceOwnerKnowledgeImage, types.ResourceRelationExtractedImage).Count(&trustedMarkers).Error)
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ?", forged.ID,
		types.ResourceOwnerKnowledgeImage).Count(&forgedMarkers).Error)
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type = ?", forged.ID,
		types.ResourceOwnerKnowledgeChunk).Count(&chunkClaims).Error)
	require.EqualValues(t, 1, trustedMarkers)
	require.Zero(t, forgedMarkers)
	require.Zero(t, chunkClaims)
}

func TestProcessChunksRollsBackPersistedRowsWhenSecondImageMarkerFails(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	tenant := &types.Tenant{ID: 7}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, tenant)
	one, err := catalog.Register(ctx, 7, "local://7/exports/one.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	two, err := catalog.Register(ctx, 7, "local://7/exports/two.png", interfaces.ResourceRegistration{Kind: "image"})
	require.NoError(t, err)
	first, second := one, two
	if second < first {
		first, second = second, first
	}
	require.NoError(t, db.AutoMigrate(&types.KnowledgeBase{}, &types.Knowledge{}, &types.Chunk{}, &types.WikiPage{}))
	require.NoError(t, db.Create(&types.KnowledgeBase{ID: "kb", TenantID: 7}).Error)
	knowledge := &types.Knowledge{ID: "doc", TenantID: 7, KnowledgeBaseID: "kb", ParseStatus: types.ParseStatusProcessing}
	require.NoError(t, db.Create(knowledge).Error)
	chunkService := &parentChildChunkService{}
	wrapped := failingImageCatalog{ResourceCatalog: catalog, bindOwner: types.ResourceOwnerKnowledgeImage, bindRef: second}
	svc := &knowledgeService{repo: &parentChildKnowledgeRepo{knowledge: knowledge}, chunkRepo: chunkService,
		resourceCatalog: wrapped, task: parentChildTaskEnqueuer{}, graphEngine: parentChildGraphRepo{}, tenantRepo: parentChildTenantRepo{}}
	content := "![one](" + one + ") ![two](" + two + ")"
	svc.processChunks(ctx, &types.KnowledgeBase{ID: "kb", TenantID: 7}, knowledge,
		[]types.ParsedChunk{{Content: content, Seq: 0}}, ProcessChunksOptions{
			StoredImages: []docparser.StoredImage{{ServingURL: one}, {ServingURL: two}},
		})
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	require.Equal(t, 2, chunkService.deleteCalls, "existing rows and partial new rows are both deleted")
	for _, ref := range []string{first, second} {
		resource, resolveErr := catalog.Resolve(ctx, ref)
		require.NoError(t, resolveErr)
		var claims int64
		require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ? AND owner_type IN ?", resource.ID,
			[]string{types.ResourceOwnerKnowledgeImage, types.ResourceOwnerKnowledgeChunk}).Count(&claims).Error)
		require.Zero(t, claims, "no image claims survive parser binding rollback")
	}
}

func (r *parentChildKnowledgeRepo) GetKnowledgeByID(
	context.Context, uint64, string,
) (*types.Knowledge, error) {
	return r.knowledge, nil
}

func (r *parentChildKnowledgeRepo) UpdateKnowledge(
	context.Context, *types.Knowledge,
) error {
	return nil
}

type parentChildChunkService struct {
	interfaces.ChunkRepository
	created      []*types.Chunk
	imageInfos   []interfaces.ChunkImageInfo
	imageInfoErr error
	deleteErr    error
	deleteCalls  int
}

func (s *parentChildChunkService) DeleteChunksByKnowledgeID(context.Context, uint64, string) error {
	s.deleteCalls++
	if s.deleteErr == nil {
		s.created = nil
	}
	return s.deleteErr
}

func (s *parentChildChunkService) ListImageInfoByKnowledgeIDs(context.Context, uint64, []string) ([]interfaces.ChunkImageInfo, error) {
	return s.imageInfos, s.imageInfoErr
}

func (s *parentChildChunkService) CreateChunks(_ context.Context, chunks []*types.Chunk) error {
	s.created = append([]*types.Chunk(nil), chunks...)
	return nil
}

type parentChildModelService struct {
	interfaces.ModelService
	embedder embedding.Embedder
}

func (s parentChildModelService) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return s.embedder, nil
}

type parentChildEmbedder struct{}

func (parentChildEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1}, nil
}

func (parentChildEmbedder) BatchEmbed(context.Context, []string) ([][]float32, error) {
	return [][]float32{{1}}, nil
}

func (parentChildEmbedder) BatchEmbedWithPool(
	context.Context, embedding.Embedder, []string,
) ([][]float32, error) {
	return [][]float32{{1}}, nil
}

func (parentChildEmbedder) GetModelName() string { return "parent-child-test" }
func (parentChildEmbedder) GetDimensions() int   { return 1 }
func (parentChildEmbedder) GetModelID() string   { return "parent-child-test" }

type parentChildRetrieveEngine struct {
	interfaces.RetrieveEngineService
	indexed []*types.IndexInfo
}

func (e *parentChildRetrieveEngine) EngineType() types.RetrieverEngineType {
	return types.PostgresRetrieverEngineType
}

func (e *parentChildRetrieveEngine) Support() []types.RetrieverType {
	return []types.RetrieverType{types.VectorRetrieverType}
}

func (e *parentChildRetrieveEngine) DeleteByKnowledgeIDList(
	context.Context, []string, int, string,
) error {
	return nil
}

func (e *parentChildRetrieveEngine) EstimateStorageSize(
	context.Context, embedding.Embedder, []*types.IndexInfo, []types.RetrieverType,
) int64 {
	return 0
}

func (e *parentChildRetrieveEngine) BatchIndex(
	_ context.Context,
	_ embedding.Embedder,
	infos []*types.IndexInfo,
	_ []types.RetrieverType,
) error {
	e.indexed = append([]*types.IndexInfo(nil), infos...)
	return nil
}

type parentChildRetrieveRegistry struct {
	interfaces.RetrieveEngineRegistry
	engine interfaces.RetrieveEngineService
}

func (r parentChildRetrieveRegistry) GetRetrieveEngineService(
	types.RetrieverEngineType,
) (interfaces.RetrieveEngineService, error) {
	return r.engine, nil
}

type parentChildGraphRepo struct {
	interfaces.RetrieveGraphRepository
}

func (parentChildGraphRepo) DelGraph(context.Context, []types.NameSpace) error {
	return nil
}

type parentChildTenantRepo struct {
	interfaces.TenantRepository
}

func (parentChildTenantRepo) AdjustStorageUsed(context.Context, uint64, int64) error {
	return nil
}

type parentChildTaskEnqueuer struct{}

func (parentChildTaskEnqueuer) Enqueue(*asynq.Task, ...asynq.Option) (*asynq.TaskInfo, error) {
	return nil, nil
}

func TestProcessChunksIndexesEveryTextChild(t *testing.T) {
	knowledge := &types.Knowledge{
		ID:              "knowledge-1",
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		ParseStatus:     types.ParseStatusProcessing,
	}
	chunkService := &parentChildChunkService{}
	retrieveEngine := &parentChildRetrieveEngine{}
	tenant := &types.Tenant{
		ID: 1,
		RetrieverEngines: types.RetrieverEngines{Engines: []types.RetrieverEngineParams{
			{
				RetrieverType:       types.VectorRetrieverType,
				RetrieverEngineType: types.PostgresRetrieverEngineType,
			},
		}},
	}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, tenant)
	svc := &knowledgeService{
		repo:           &parentChildKnowledgeRepo{knowledge: knowledge},
		chunkRepo:      chunkService,
		modelService:   parentChildModelService{embedder: parentChildEmbedder{}},
		retrieveEngine: parentChildRetrieveRegistry{engine: retrieveEngine},
		graphEngine:    parentChildGraphRepo{},
		tenantRepo:     parentChildTenantRepo{},
		task:           parentChildTaskEnqueuer{},
	}
	kb := &types.KnowledgeBase{
		ID:               "kb-1",
		TenantID:         1,
		EmbeddingModelID: "embedding-1",
		IndexingStrategy: types.IndexingStrategy{VectorEnabled: true},
	}
	chunks := []types.ParsedChunk{
		{Content: "linked child", Seq: 0, Start: 0, End: 12, ParentIndex: 0},
		{Content: "standalone child", Seq: 1, Start: 12, End: 28, ParentIndex: -1},
	}

	svc.processChunks(ctx, kb, knowledge, chunks, ProcessChunksOptions{
		ParentChunks: []types.ParsedParentChunk{
			{Content: "parent context", Seq: 0, Start: 0, End: 28},
		},
	})

	var textChunkIDs []string
	for _, chunk := range chunkService.created {
		if chunk.ChunkType == types.ChunkTypeText {
			textChunkIDs = append(textChunkIDs, chunk.ID)
		}
	}
	require.Len(t, textChunkIDs, 2)

	indexedSourceIDs := make([]string, 0, len(retrieveEngine.indexed))
	for _, info := range retrieveEngine.indexed {
		indexedSourceIDs = append(indexedSourceIDs, info.SourceID)
	}
	require.ElementsMatch(t, textChunkIDs, indexedSourceIDs)
}

func TestProcessChunksPreservesExistingRowsWhenImageReferencesCannotBeRead(t *testing.T) {
	knowledge := &types.Knowledge{
		ID: "knowledge-1", TenantID: 1, KnowledgeBaseID: "kb-1", ParseStatus: types.ParseStatusProcessing,
	}
	chunkService := &parentChildChunkService{imageInfoErr: errors.New("temporary database failure")}
	service := &knowledgeService{
		repo:      &parentChildKnowledgeRepo{knowledge: knowledge},
		chunkRepo: chunkService,
	}
	service.processChunks(context.Background(), &types.KnowledgeBase{ID: "kb-1", TenantID: 1}, knowledge,
		[]types.ParsedChunk{{Content: "new content", Seq: 0}}, ProcessChunksOptions{})

	require.Zero(t, chunkService.deleteCalls, "existing chunks must remain so their image bindings can be retried")
	require.Empty(t, chunkService.created, "new chunks must not be written after cleanup preflight fails")
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	require.Equal(t, "failed to collect existing chunk image references", knowledge.ErrorMessage)
}

func TestProcessChunksStopsWhenExistingChunkDeletionFails(t *testing.T) {
	knowledge := &types.Knowledge{
		ID: "knowledge-1", TenantID: 1, KnowledgeBaseID: "kb-1", ParseStatus: types.ParseStatusProcessing,
	}
	chunkService := &parentChildChunkService{deleteErr: errors.New("chunk delete failed")}
	service := &knowledgeService{
		repo:      &parentChildKnowledgeRepo{knowledge: knowledge},
		chunkRepo: chunkService,
	}
	service.processChunks(context.Background(), &types.KnowledgeBase{ID: "kb-1", TenantID: 1}, knowledge,
		[]types.ParsedChunk{{Content: "new content", Seq: 0}}, ProcessChunksOptions{})

	require.Equal(t, 1, chunkService.deleteCalls)
	require.Empty(t, chunkService.created, "new chunks must not be appended to uncleared rows")
	require.Equal(t, types.ParseStatusFailed, knowledge.ParseStatus)
	require.Equal(t, "failed to remove existing chunks before reprocessing", knowledge.ErrorMessage)
}
