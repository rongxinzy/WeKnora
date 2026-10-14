package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/models/embedding"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type kbTaskCancelCall struct {
	kbID          string
	knowledgeIDs  []string
	dataSourceIDs []string
}

type recordingKBTaskInspector struct {
	repo                 *kbDeleteKBRepo
	calls                []kbTaskCancelCall
	cancelErr            error
	sawSoftDeletedRecord bool
}

func (r *recordingKBTaskInspector) CancelTasksForKnowledge(
	context.Context,
	string,
) (int, int, error) {
	return 0, 0, nil
}

func (r *recordingKBTaskInspector) HasQueuedDeleteTasksForKnowledge(context.Context, string) (bool, error) {
	return false, nil
}

func (r *recordingKBTaskInspector) HasQueuedTasksForKnowledge(context.Context, string) (bool, error) {
	return false, nil
}

func (r *recordingKBTaskInspector) QueueStats(context.Context) ([]types.QueueStat, bool, error) {
	return nil, true, nil
}

func (r *recordingKBTaskInspector) WorkerServerStats(context.Context) ([]types.WorkerServerStat, bool, error) {
	return nil, true, nil
}

func (r *recordingKBTaskInspector) CancelTasksForKnowledgeBase(
	_ context.Context,
	kbID string,
	knowledgeIDs []string,
	dataSourceIDs []string,
) (int, int, error) {
	r.calls = append(r.calls, kbTaskCancelCall{
		kbID:          kbID,
		knowledgeIDs:  append([]string(nil), knowledgeIDs...),
		dataSourceIDs: append([]string(nil), dataSourceIDs...),
	})
	if r.repo != nil && r.repo.deletedID == kbID {
		r.sawSoftDeletedRecord = true
	}
	return 0, 0, r.cancelErr
}

var (
	_ interfaces.TaskInspector              = (*recordingKBTaskInspector)(nil)
	_ interfaces.KnowledgeBaseTaskCanceller = (*recordingKBTaskInspector)(nil)
)

type recordingKBDeleteEnqueuer struct {
	calls int
	task  *asynq.Task
}

type recordingKBPendingRepo struct {
	interfaces.TaskPendingOpsRepository
	scopeIDs  []string
	deleteErr error
}

func (r *recordingKBPendingRepo) DeleteByScope(_ context.Context, scope, scopeID string) error {
	if scope == types.TaskScopeKnowledgeBase {
		r.scopeIDs = append(r.scopeIDs, scopeID)
	}
	return r.deleteErr
}

func (r *recordingKBDeleteEnqueuer) Enqueue(
	task *asynq.Task,
	_ ...asynq.Option,
) (*asynq.TaskInfo, error) {
	r.calls++
	r.task = task
	return &asynq.TaskInfo{ID: "kb-delete-task"}, nil
}

func TestDeleteKnowledgeBaseForwardsDataSourceTaskScope(t *testing.T) {
	const kbID = "kb-with-datasource"
	kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
	kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}
	inspector := &recordingKBTaskInspector{repo: kbRepo}
	enqueuer := &recordingKBDeleteEnqueuer{}
	dsRepo := newKBDeleteDSRepo(kbID, &types.DataSource{ID: "datasource-1", KnowledgeBaseID: kbID})
	svc := &knowledgeBaseService{
		repo:          kbRepo,
		asynqClient:   enqueuer,
		taskInspector: inspector,
		dsRepo:        dsRepo,
	}

	err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)

	require.NoError(t, err)
	require.Len(t, inspector.calls, 2)
	assert.Empty(t, inspector.calls[0].dataSourceIDs)
	assert.Equal(t, []string{"datasource-1"}, inspector.calls[1].dataSourceIDs)
	require.NotNil(t, enqueuer.task)
	var payload types.KBDeletePayload
	require.NoError(t, json.Unmarshal(enqueuer.task.Payload(), &payload))
	assert.Equal(t, []string{"datasource-1"}, payload.DataSourceIDs)
}

func TestDeleteKnowledgeBaseCancelsQueuedTasksBestEffort(t *testing.T) {
	tests := []struct {
		name       string
		cancelErr  error
		pendingErr error
	}{
		{name: "success"},
		{name: "inspector failure", cancelErr: errors.New("redis unavailable")},
		{name: "durable queue failure", pendingErr: errors.New("database unavailable")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const kbID = "kb-task-cleanup"
			kbRepo := &kbDeleteKBRepo{fakeKBRepo: *newFakeKBRepo()}
			kbRepo.rows[kbID] = &types.KnowledgeBase{ID: kbID, TenantID: 1, Name: "test"}
			inspector := &recordingKBTaskInspector{repo: kbRepo, cancelErr: tt.cancelErr}
			pendingRepo := &recordingKBPendingRepo{deleteErr: tt.pendingErr}
			enqueuer := &recordingKBDeleteEnqueuer{}
			svc := &knowledgeBaseService{
				repo:            kbRepo,
				asynqClient:     enqueuer,
				taskInspector:   inspector,
				taskPendingRepo: pendingRepo,
			}

			err := svc.DeleteKnowledgeBase(ctxWithTenantStorage(1, "local"), kbID)

			require.NoError(t, err)
			require.Len(t, inspector.calls, 1)
			assert.Equal(t, kbID, inspector.calls[0].kbID)
			assert.Empty(t, inspector.calls[0].knowledgeIDs)
			assert.True(t, inspector.sawSoftDeletedRecord)
			assert.Equal(t, []string{kbID}, pendingRepo.scopeIDs)
			assert.Equal(t, 1, enqueuer.calls)
		})
	}
}

type emptyKBKnowledgeRepo struct {
	interfaces.KnowledgeRepository
}

func (emptyKBKnowledgeRepo) ListKnowledgeByKnowledgeBaseID(
	context.Context,
	uint64,
	string,
) ([]*types.Knowledge, error) {
	return nil, nil
}

func TestProcessKBDeleteRepeatsQueueCleanup(t *testing.T) {
	inspector := &recordingKBTaskInspector{}
	pendingRepo := &recordingKBPendingRepo{}
	svc := &knowledgeBaseService{
		kgRepo:          emptyKBKnowledgeRepo{},
		taskInspector:   inspector,
		taskPendingRepo: pendingRepo,
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 1, KnowledgeBaseID: "kb-race"})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.NoError(t, err)
	require.Len(t, inspector.calls, 2)
	for _, call := range inspector.calls {
		assert.Equal(t, "kb-race", call.kbID)
		assert.Empty(t, call.knowledgeIDs)
	}
	assert.Equal(t, []string{"kb-race", "kb-race"}, pendingRepo.scopeIDs)
}

type populatedKBKnowledgeRepo struct {
	interfaces.KnowledgeRepository
	items       []*types.Knowledge
	deleteCalls *int
}

func (r populatedKBKnowledgeRepo) ListKnowledgeByKnowledgeBaseID(
	context.Context,
	uint64,
	string,
) ([]*types.Knowledge, error) {
	return r.items, nil
}

func (r populatedKBKnowledgeRepo) DeleteKnowledgeList(context.Context, uint64, []string) error {
	if r.deleteCalls != nil {
		*r.deleteCalls = *r.deleteCalls + 1
	}
	return nil
}

type kbCleanupChunkRepo struct {
	interfaces.ChunkRepository
	imageInfoErr error
	deleteCalls  *int
	deleteErrFor string
	imageRows    []interfaces.ChunkImageInfo
}

func (r kbCleanupChunkRepo) ListImageInfoByKnowledgeIDs(
	context.Context,
	uint64,
	[]string,
) ([]interfaces.ChunkImageInfo, error) {
	return r.imageRows, r.imageInfoErr
}

func (r kbCleanupChunkRepo) DeleteChunksByKnowledgeID(_ context.Context, _ uint64, knowledgeID string) error {
	if r.deleteCalls != nil {
		*r.deleteCalls = *r.deleteCalls + 1
	}
	if knowledgeID == r.deleteErrFor {
		return errors.New("chunk deletion failed for " + knowledgeID)
	}
	return nil
}

type kbCleanupResourceCatalogStub struct {
	interfaces.ResourceCatalog
	releases []types.ResourceBinding
}

func (s *kbCleanupResourceCatalogStub) Release(_ context.Context, reference, ownerType, ownerID string) (int64, error) {
	s.releases = append(s.releases, types.ResourceBinding{ResourceID: reference, OwnerType: ownerType, OwnerID: ownerID})
	return 1, nil
}

func TestProcessKBDeletePreservesChunksWhenImageReferencesCannotBeRead(t *testing.T) {
	const kbID = "kb-image-info-fail"
	deleteCalls := 0
	imageErr := errors.New("image-info query failed")
	svc := &knowledgeBaseService{
		kgRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
			{ID: "k1", KnowledgeBaseID: kbID, EmbeddingModelID: "m1"},
		}},
		chunkRepo:     kbCleanupChunkRepo{imageInfoErr: imageErr, deleteCalls: &deleteCalls},
		modelService:  kbCleanupModelService{},
		taskInspector: &recordingKBTaskInspector{},
	}

	err := svc.ProcessKBDelete(context.Background(), kbDeletePayload(t, kbID, 1))
	require.ErrorIs(t, err, imageErr)
	require.Zero(t, deleteCalls, "failed image reference enumeration must leave chunks for task retry")
}

func TestProcessKBDeletePartialChunkFailureRetainsParentRowsAndReleasesOnlyDeletedBindings(t *testing.T) {
	const kbID = "kb-partial-chunk-delete"
	deleteCalls := 0
	knowledgeDeleteCalls := 0
	catalog := &kbCleanupResourceCatalogStub{}
	files := &documentFileSpy{}
	firstRef := types.BuildResourcePath("abcdefghijklmnopqrstuv")
	secondRef := types.BuildResourcePath("bcdefghijklmnopqrstuvw")
	svc := &knowledgeBaseService{
		kgRepo: populatedKBKnowledgeRepo{
			items: []*types.Knowledge{
				{ID: "k1", KnowledgeBaseID: kbID, FilePath: "source-k1"},
				{ID: "k2", KnowledgeBaseID: kbID, FilePath: "source-k2"},
			},
			deleteCalls: &knowledgeDeleteCalls,
		},
		chunkRepo: kbCleanupChunkRepo{
			deleteCalls:  &deleteCalls,
			deleteErrFor: "k2",
			imageRows: []interfaces.ChunkImageInfo{
				{ChunkID: "chunk-1", KnowledgeID: "k1", ImageInfo: `[{"url":"` + firstRef + `"}]`},
				{ChunkID: "chunk-2", KnowledgeID: "k2", ImageInfo: `[{"url":"` + secondRef + `"}]`},
			},
		},
		resourceCatalog: catalog,
		fileSvc:         files,
		modelService:    kbCleanupModelService{},
		taskInspector:   &recordingKBTaskInspector{},
	}

	err := svc.ProcessKBDelete(context.Background(), kbDeletePayload(t, kbID, 1))
	require.ErrorContains(t, err, "chunk deletion failed for k2")
	require.Equal(t, 2, deleteCalls)
	require.Zero(t, knowledgeDeleteCalls, "knowledge rows must remain until every chunk delete succeeds")
	require.Empty(t, files.deleted, "original source files must remain until every chunk delete succeeds")
	require.Equal(t, []types.ResourceBinding{
		{ResourceID: firstRef, OwnerType: types.ResourceOwnerKnowledgeChunk, OwnerID: "chunk-1"},
		// Extracted-image provenance is a separate knowledge_image owner
		// (relation=extracted_image); never release the source-file knowledge owner.
		{ResourceID: firstRef, OwnerType: types.ResourceOwnerKnowledgeImage, OwnerID: "k1"},
	}, catalog.releases, "only chunk-image and extracted-image marker owners for successfully deleted documents are released")
}

type kbCleanupModelService struct {
	interfaces.ModelService
}

func (kbCleanupModelService) GetEmbeddingModel(context.Context, string) (embedding.Embedder, error) {
	return kbCleanupEmbedder{}, nil
}

type kbCleanupEmbedder struct{}

func (kbCleanupEmbedder) Embed(context.Context, string) ([]float32, error) { return nil, nil }
func (kbCleanupEmbedder) BatchEmbed(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (kbCleanupEmbedder) GetModelName() string { return "test" }
func (kbCleanupEmbedder) GetDimensions() int   { return 1 }
func (kbCleanupEmbedder) GetModelID() string   { return "test" }
func (kbCleanupEmbedder) BatchEmbedWithPool(
	context.Context,
	embedding.Embedder,
	[]string,
) ([][]float32, error) {
	return nil, nil
}

func TestProcessKBDeleteCollectsKnowledgeIDsForEveryScrub(t *testing.T) {
	inspector := &recordingKBTaskInspector{}
	svc := &knowledgeBaseService{
		kgRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
			{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
			{ID: "knowledge-2", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
		}},
		chunkRepo:     kbCleanupChunkRepo{},
		modelService:  kbCleanupModelService{},
		taskInspector: inspector,
	}
	payload, err := json.Marshal(types.KBDeletePayload{TenantID: 1, KnowledgeBaseID: "kb-1"})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.NoError(t, err)
	require.Len(t, inspector.calls, 2)
	for _, call := range inspector.calls {
		assert.Equal(t, []string{"knowledge-1", "knowledge-2"}, call.knowledgeIDs)
	}
}

// kbDeleteDeferredRegistry reports a retryable engine-resolution failure from
// the rebuild path, matching what GetOrLoadByStoreID does when the caller
// goes away or the store engine cannot be produced yet.
type kbDeleteDeferredRegistry struct {
	err error
}

func (kbDeleteDeferredRegistry) Register(interfaces.RetrieveEngineService) error { return nil }
func (kbDeleteDeferredRegistry) GetRetrieveEngineService(types.RetrieverEngineType) (
	interfaces.RetrieveEngineService, error,
) {
	return nil, nil
}
func (kbDeleteDeferredRegistry) GetAllRetrieveEngineServices() []interfaces.RetrieveEngineService {
	return nil
}
func (kbDeleteDeferredRegistry) GetByStoreID(string) (interfaces.RetrieveEngineService, error) {
	return nil, errors.New("store not in registry")
}
func (r kbDeleteDeferredRegistry) GetOrLoadByStoreID(
	context.Context, uint64, string,
) (interfaces.RetrieveEngineService, error) {
	return nil, r.err
}

type kbDeleteOwnership struct {
	owned map[string]uint64
}

func (o *kbDeleteOwnership) StoreOwnedBy(_ context.Context, storeID string, tenantID uint64) (bool, error) {
	owner, ok := o.owned[storeID]
	return ok && owner == tenantID, nil
}

type kbDeleteTrackingKnowledgeRepo struct {
	populatedKBKnowledgeRepo
	deleteCalls int
}

func (r *kbDeleteTrackingKnowledgeRepo) DeleteKnowledgeList(context.Context, uint64, []string) error {
	r.deleteCalls++
	return nil
}

func TestProcessKBDeleteEngineResolutionFailureRetries(t *testing.T) {
	const storeID = "00000000-0000-0000-0000-0000000000dd"
	storeIDPtr := storeID
	repo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
		{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
	}}}
	svc := &knowledgeBaseService{
		kgRepo:         repo,
		chunkRepo:      kbCleanupChunkRepo{},
		modelService:   kbCleanupModelService{},
		retrieveEngine: kbDeleteDeferredRegistry{err: context.Canceled},
		ownership:      &kbDeleteOwnership{owned: map[string]uint64{storeID: 1}},
	}
	payload, err := json.Marshal(types.KBDeletePayload{
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		VectorStoreID:   &storeIDPtr,
	})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, repo.deleteCalls, "knowledge rows must not be deleted when engine resolution is deferred")
}

func TestProcessKBDeleteUnavailableStoreRetries(t *testing.T) {
	const storeID = "00000000-0000-0000-0000-0000000000ee"
	storeIDPtr := storeID
	repo := &kbDeleteTrackingKnowledgeRepo{populatedKBKnowledgeRepo: populatedKBKnowledgeRepo{items: []*types.Knowledge{
		{ID: "knowledge-1", KnowledgeBaseID: "kb-1", EmbeddingModelID: "model-1"},
	}}}
	svc := &knowledgeBaseService{
		kgRepo:         repo,
		chunkRepo:      kbCleanupChunkRepo{},
		modelService:   kbCleanupModelService{},
		retrieveEngine: kbDeleteDeferredRegistry{err: retriever.ErrVectorStoreUnavailable},
		ownership:      &kbDeleteOwnership{owned: map[string]uint64{storeID: 1}},
	}
	payload, err := json.Marshal(types.KBDeletePayload{
		TenantID:        1,
		KnowledgeBaseID: "kb-1",
		VectorStoreID:   &storeIDPtr,
	})
	require.NoError(t, err)

	err = svc.ProcessKBDelete(context.Background(), asynq.NewTask(types.TypeKBDelete, payload))

	require.ErrorIs(t, err, retriever.ErrVectorStoreUnavailable)
	assert.Equal(t, 0, repo.deleteCalls, "knowledge rows must not be deleted when engine resolution is deferred")
}

func TestCancelTasksForKnowledgeBaseForwardsKnowledgeIDs(t *testing.T) {
	inspector := &recordingKBTaskInspector{}
	svc := &knowledgeBaseService{taskInspector: inspector}

	svc.cancelTasksForKnowledgeBase(
		context.Background(),
		"kb-1",
		[]string{"knowledge-1", "knowledge-2"},
		[]string{"datasource-1"},
	)

	require.Len(t, inspector.calls, 1)
	assert.Equal(t, "kb-1", inspector.calls[0].kbID)
	assert.Equal(t, []string{"knowledge-1", "knowledge-2"}, inspector.calls[0].knowledgeIDs)
	assert.Equal(t, []string{"datasource-1"}, inspector.calls[0].dataSourceIDs)
}
