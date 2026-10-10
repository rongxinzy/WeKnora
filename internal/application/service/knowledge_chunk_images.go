package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
)

type pendingChunkImageBinding struct {
	chunkID string
	refs    []string
}

type contentResourceTransitionClaim struct {
	ref     string
	ownerID string
}

func holdReplacementResourcesBeforeCleanup(
	ctx context.Context, catalog interfaces.ResourceCatalog, fileSvc interfaces.FileService,
	tenantID uint64, content string, cleanup func() error,
) ([]contentResourceTransitionClaim, error) {
	claims, err := holdContentResourceClaims(ctx, catalog, fileSvc, tenantID, content)
	if err != nil {
		return nil, err
	}
	if cleanup != nil {
		if err := cleanup(); err != nil {
			return claims, err
		}
	}
	return claims, nil
}

// holdContentResourceClaims protects resources referenced by replacement
// manual content while cleanup removes the old document's chunk/knowledge
// bindings. The temporary owner is released after synchronous processing has
// persisted the replacement chunk bindings.
func holdContentResourceClaims(
	ctx context.Context, catalog interfaces.ResourceCatalog, fileSvc interfaces.FileService, tenantID uint64, content string,
) ([]contentResourceTransitionClaim, error) {
	if catalog == nil || tenantID == 0 {
		return nil, nil
	}
	refs := types.ScanResourceReferences(content)
	claims := make([]contentResourceTransitionClaim, 0, len(refs))
	for _, ref := range refs {
		resource, err := catalog.Resolve(ctx, ref)
		if err != nil {
			releaseContentResourceClaims(ctx, catalog, fileSvc, claims)
			return nil, fmt.Errorf("resolve replacement content resource: %w", err)
		}
		if resource == nil || resource.TenantID != tenantID || resource.State != types.ResourceStateActive {
			continue
		}
		ownerID := uuid.NewString()
		if err := catalog.Bind(ctx, ref, types.ResourceOwnerTemporaryDocument, ownerID, types.ResourceRelationAttachment); err != nil {
			releaseContentResourceClaims(ctx, catalog, fileSvc, claims)
			return nil, fmt.Errorf("hold replacement content resource: %w", err)
		}
		claims = append(claims, contentResourceTransitionClaim{ref: ref, ownerID: ownerID})
	}
	return claims, nil
}

func releaseContentResourceClaims(
	ctx context.Context, catalog interfaces.ResourceCatalog, fileSvc interfaces.FileService,
	claims []contentResourceTransitionClaim,
) {
	if catalog == nil {
		return
	}
	for _, claim := range claims {
		remaining, err := catalog.Release(ctx, claim.ref, types.ResourceOwnerTemporaryDocument, claim.ownerID)
		if err != nil {
			logger.Warnf(ctx, "Failed to release temporary replacement content resource claim: %v", err)
			continue
		}
		if remaining == 0 && fileSvc != nil {
			if err := fileSvc.DeleteFile(ctx, claim.ref); err != nil {
				logger.Warnf(ctx, "Failed to delete unreferenced replacement content resource: %v", err)
			}
		}
	}
}

// prepareChunkImageInfo projects only stored image resources referenced by the
// chunk's rendered Markdown/HTML. External URLs and arbitrary provider paths
// remain ordinary content and are never made downloadable through this API.
func prepareChunkImageInfo(ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, content string) (string, []string) {
	if catalog == nil || tenantID == 0 {
		return "", nil
	}
	urls := searchutil.ImageURLsInContent(content)
	refs := make([]string, 0, len(urls))
	for ref := range urls {
		if _, ok := types.ParseResourcePath(ref); !ok {
			continue
		}
		resource, err := catalog.Resolve(ctx, ref)
		if err != nil || resource == nil || resource.TenantID != tenantID ||
			resource.Kind != "image" || resource.State != types.ResourceStateActive {
			continue
		}
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	if len(refs) == 0 {
		return "", nil
	}
	infos := make([]types.ImageInfo, 0, len(refs))
	for _, ref := range refs {
		infos = append(infos, types.ImageInfo{URL: ref})
	}
	data, err := json.Marshal(infos)
	if err != nil {
		logger.Warnf(ctx, "Failed to encode stored chunk image references: %v", err)
		return "", nil
	}
	return string(data), refs
}

func bindChunkImageResource(ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, chunkID, ref string) error {
	if catalog == nil || tenantID == 0 || chunkID == "" {
		return fmt.Errorf("chunk image binding requires catalog, tenant, and chunk")
	}
	if _, ok := types.ParseResourcePath(ref); !ok {
		return fmt.Errorf("chunk image must be a registered resource handle")
	}
	resource, err := catalog.Resolve(ctx, ref)
	if err != nil {
		return err
	}
	if resource == nil || resource.TenantID != tenantID || resource.Kind != "image" || resource.State != types.ResourceStateActive {
		return fmt.Errorf("chunk image resource is not active in the owning tenant")
	}
	return catalog.Bind(ctx, ref, types.ResourceOwnerKnowledgeChunk, chunkID, types.ResourceRelationChunkImage)
}

// bindChunkImageResourceIfStored preserves legacy image metadata writes while
// granting download capability only to active resource handles.
func bindChunkImageResourceIfStored(ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, chunkID, ref string) error {
	if catalog == nil {
		return nil
	}
	if _, ok := types.ParseResourcePath(ref); !ok {
		return nil
	}
	return bindChunkImageResource(ctx, catalog, tenantID, chunkID, ref)
}

func bindChunkImageInfo(ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, chunkID, imageInfo string) error {
	if catalog == nil || imageInfo == "" {
		return nil
	}
	var images []types.ImageInfo
	if err := json.Unmarshal([]byte(imageInfo), &images); err != nil {
		return fmt.Errorf("decode chunk image info: %w", err)
	}
	var bound []string
	for _, image := range images {
		if _, ok := types.ParseResourcePath(image.URL); !ok {
			// Legacy provider paths and external URLs remain descriptive metadata.
			// They are never promoted to an authorized image-download binding.
			continue
		}
		if err := bindChunkImageResource(ctx, catalog, tenantID, chunkID, image.URL); err != nil {
			for _, ref := range bound {
				_, _ = catalog.Release(ctx, ref, types.ResourceOwnerKnowledgeChunk, chunkID)
			}
			return err
		}
		bound = append(bound, image.URL)
	}
	return nil
}

func releaseChunkImageResources(
	ctx context.Context,
	catalog interfaces.ResourceCatalog,
	fileSvc interfaces.FileService,
	rows []interfaces.ChunkImageInfo,
	knowledgeIDs []string,
) {
	if len(rows) == 0 {
		return
	}
	for _, row := range rows {
		if row.ImageInfo == "" {
			continue
		}
		var infos []types.ImageInfo
		if err := json.Unmarshal([]byte(row.ImageInfo), &infos); err != nil {
			logger.Warnf(ctx, "Failed to decode chunk image bindings during cleanup: %v", err)
			continue
		}
		for _, info := range infos {
			ref := info.URL
			if ref == "" {
				continue
			}
			if catalog == nil {
				logger.Warnf(ctx, "Skipping unregistered legacy chunk image cleanup without ownership proof")
				continue
			}
			if _, ok := types.ParseResourcePath(ref); !ok {
				// Raw provider paths do not prove resource ownership. Preserve them
				// as potential orphans rather than deleting an arbitrary object.
				logger.Warnf(ctx, "Skipping unregistered legacy chunk image cleanup without ownership proof")
				continue
			}
			remaining := int64(-1)
			if row.ChunkID != "" {
				count, err := catalog.Release(ctx, ref, types.ResourceOwnerKnowledgeChunk, row.ChunkID)
				if err != nil {
					logger.Warnf(ctx, "Failed to release chunk image binding: %v", err)
					continue
				}
				remaining = count
			}
			for _, knowledgeID := range knowledgeIDs {
				count, err := catalog.Release(ctx, ref, types.ResourceOwnerKnowledge, knowledgeID)
				if err != nil {
					logger.Warnf(ctx, "Failed to release legacy knowledge image binding: %v", err)
					remaining = 1 // fail closed: retain bytes
					break
				}
				remaining = count
			}
			if remaining == 0 && fileSvc != nil {
				if err := fileSvc.DeleteFile(ctx, ref); err != nil {
					logger.Warnf(ctx, "Failed to delete unreferenced extracted image: %v", err)
				}
			}
		}
	}
}
