package service

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type pendingChunkImageBinding struct {
	chunkID string
	refs    []string
}

type contentResourceTransitionClaim struct {
	ref     string
	ownerID string
}

var resourceReferenceForSanitizing = regexp.MustCompile(`(?i)resource://[A-Za-z0-9_-]+`)

func canonicalResourceReference(value string) (string, bool) {
	decoded := value
	for i := 0; i < 3; i++ {
		next := html.UnescapeString(decoded)
		if unescaped, err := url.PathUnescape(next); err == nil {
			next = unescaped
		}
		if next == decoded {
			break
		}
		decoded = next
	}
	match := resourceReferenceForSanitizing.FindString(decoded)
	if match == "" {
		return "", false
	}
	canonical := types.ResourceScheme + match[len(types.ResourceScheme):]
	if _, ok := types.ParseResourcePath(canonical); !ok {
		return "", false
	}
	return canonical, true
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
	// Rich-text bodies are caller controlled. A handle mentioned in the body is
	// not evidence that this document owns it, so never create a temporary claim
	// from raw content. Images produced by the parser are claimed later from the
	// resolver's explicit StoredImages allowlist.
	_ = catalog
	_ = fileSvc
	_ = tenantID
	_ = content
	return nil, nil
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
func prepareChunkImageInfo(
	ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, content string, trustedRefs map[string]struct{},
) (string, []string) {
	if catalog == nil || tenantID == 0 {
		return "", nil
	}
	urls := searchutil.ImageURLsInContent(content)
	refs := make([]string, 0, len(urls))
	for ref := range urls {
		if _, ok := types.ParseResourcePath(ref); !ok {
			continue
		}
		if _, ok := trustedRefs[ref]; !ok {
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

// trustedStoredImageRefs derives provenance only from handles returned by the
// server-side image resolver during this processing attempt. Content itself is
// never an authority source for a resource handle.
func trustedStoredImageRefs(images []docparser.StoredImage) map[string]struct{} {
	refs := make(map[string]struct{}, len(images))
	for _, image := range images {
		if _, ok := types.ParseResourcePath(image.ServingURL); ok {
			refs[image.ServingURL] = struct{}{}
		}
	}
	return refs
}

// removeUntrustedResourceReferences keeps resolver-created handles but removes
// every other literal resource handle before chunking/indexing. This prevents
// search output from later turning caller-supplied handles into signed image
// URLs even when no ImageInfo entry was created.
func removeUntrustedResourceReferences(content string, trustedRefs map[string]struct{}) string {
	decoded := content
	for i := 0; i < 3; i++ {
		next := html.UnescapeString(decoded)
		if unescaped, err := url.PathUnescape(next); err == nil {
			next = unescaped
		}
		if next == decoded {
			break
		}
		decoded = next
	}
	if decoded != content && resourceReferenceForSanitizing.MatchString(decoded) {
		// If an entity/percent alias decodes to a storage handle, normalize this
		// adversarial passage before removing the handle so it cannot be signed
		// later by the generic response URL rewriter.
		content = decoded
	}
	for _, ref := range resourceReferenceForSanitizing.FindAllString(content, -1) {
		canonical, isResource := canonicalResourceReference(ref)
		if isResource {
			if _, ok := trustedRefs[canonical]; !ok {
				content = strings.ReplaceAll(content, ref, "")
			}
		}
	}
	return content
}

func bindChunkImageResource(
	ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, chunkID, ref string, trustedRefs map[string]struct{},
) error {
	if catalog == nil || tenantID == 0 || chunkID == "" {
		return fmt.Errorf("chunk image binding requires catalog, tenant, and chunk")
	}
	if _, ok := types.ParseResourcePath(ref); !ok {
		return fmt.Errorf("chunk image must be a registered resource handle")
	}
	if _, ok := trustedRefs[ref]; !ok {
		return fmt.Errorf("chunk image resource was not produced by the trusted parser")
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

func bindExtractedImageToKnowledge(
	ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, knowledgeID, ref string, trustedRefs map[string]struct{},
) error {
	if catalog == nil || tenantID == 0 || knowledgeID == "" {
		return fmt.Errorf("extracted image provenance requires catalog, tenant, and knowledge")
	}
	if _, ok := trustedRefs[ref]; !ok {
		return fmt.Errorf("extracted image was not produced by the trusted parser")
	}
	resource, err := catalog.Resolve(ctx, ref)
	if err != nil {
		return err
	}
	if resource == nil || resource.TenantID != tenantID || resource.Kind != "image" || resource.State != types.ResourceStateActive {
		return fmt.Errorf("extracted image is not active in the owning tenant")
	}
	lookup, ok := catalog.(interfaces.ResourceBindingLookup)
	if !ok {
		return fmt.Errorf("resource catalog cannot verify extracted image provenance")
	}
	hasMarker, err := lookup.HasBinding(ctx, ref, types.ResourceOwnerKnowledgeImage, knowledgeID, types.ResourceRelationExtractedImage)
	if err != nil {
		return err
	}
	if hasMarker {
		return nil
	}
	return catalog.Bind(ctx, ref, types.ResourceOwnerKnowledgeImage, knowledgeID, types.ResourceRelationExtractedImage)
}

// bindTransferredChunkImageInfo is only for clone/transfer flows after their
// caller has validated the KB transfer and copied the source image object.
func bindTransferredChunkImageInfo(ctx context.Context, catalog interfaces.ResourceCatalog, tenantID uint64, knowledgeID, chunkID, imageInfo string) error {
	if catalog == nil || imageInfo == "" {
		return nil
	}
	var images []types.ImageInfo
	if err := json.Unmarshal([]byte(imageInfo), &images); err != nil {
		return fmt.Errorf("decode chunk image info: %w", err)
	}
	var boundChunks []string
	var newMarkers []string
	rollback := func() {
		for _, ref := range boundChunks {
			_, _ = catalog.Release(ctx, ref, types.ResourceOwnerKnowledgeChunk, chunkID)
		}
		for _, ref := range newMarkers {
			_, _ = catalog.Release(ctx, ref, types.ResourceOwnerKnowledgeImage, knowledgeID)
		}
	}
	for _, image := range images {
		if _, ok := types.ParseResourcePath(image.URL); !ok {
			// Legacy provider paths and external URLs remain descriptive metadata.
			// They are never promoted to an authorized image-download binding.
			continue
		}
		resource, err := catalog.Resolve(ctx, image.URL)
		if err != nil || resource == nil || resource.TenantID != tenantID || resource.Kind != "image" || resource.State != types.ResourceStateActive {
			if err == nil {
				err = fmt.Errorf("transferred image is not active in the destination tenant")
			}
			rollback()
			return err
		}
		bindingLookup, ok := catalog.(interfaces.ResourceBindingLookup)
		if !ok {
			rollback()
			return fmt.Errorf("resource catalog cannot verify transferred image provenance")
		}
		hadMarker, err := bindingLookup.HasBinding(ctx, image.URL, types.ResourceOwnerKnowledgeImage,
			knowledgeID, types.ResourceRelationExtractedImage)
		if err != nil {
			rollback()
			return err
		}
		if err := catalog.Bind(ctx, image.URL, types.ResourceOwnerKnowledgeChunk, chunkID, types.ResourceRelationChunkImage); err != nil {
			rollback()
			return err
		}
		boundChunks = append(boundChunks, image.URL)
		if !hadMarker {
			if err := catalog.Bind(ctx, image.URL, types.ResourceOwnerKnowledgeImage, knowledgeID, types.ResourceRelationExtractedImage); err != nil {
				rollback()
				return err
			}
			newMarkers = append(newMarkers, image.URL)
		}
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
	releaseChunkImageResourcesExcept(ctx, catalog, fileSvc, rows, knowledgeIDs, nil)
}

func releaseChunkImageResourcesExcept(
	ctx context.Context,
	catalog interfaces.ResourceCatalog,
	fileSvc interfaces.FileService,
	rows []interfaces.ChunkImageInfo,
	knowledgeIDs []string,
	preservedKnowledgeImages map[string]struct{},
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
				if _, preserve := preservedKnowledgeImages[ref]; preserve {
					continue
				}
				count, err := catalog.Release(ctx, ref, types.ResourceOwnerKnowledgeImage, knowledgeID)
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
