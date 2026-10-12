package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestSearchAssemblyExcludesDisabledKnowledgeFromPrimaryAndEnrichment(t *testing.T) {
	svc := &knowledgeBaseService{}
	for _, disabled := range []bool{false, true} {
		for _, manual := range []bool{false, true} {
			status := "enabled"
			if disabled {
				status = "disabled"
			}
			knowledge := &types.Knowledge{ID: "doc", EnableStatus: status, ManualDisabled: manual}
			chunk := &types.Chunk{ID: "primary", KnowledgeID: "doc", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}
			parent := &types.Chunk{ID: "parent", KnowledgeID: "doc", ChunkType: types.ChunkTypeText, IsEnabled: true, IndexStatus: "ready"}
			inputs := []*types.IndexWithScore{{ChunkID: chunk.ID, KnowledgeID: "doc", Score: 1}}
			idx := svc.buildChunkIndex(inputs)
			idx.matchTypes[parent.ID] = types.MatchTypeParentChunk
			got := svc.assembleSearchResults(context.Background(), inputs, map[string]*types.Chunk{chunk.ID: chunk, parent.ID: parent}, map[string]*types.Knowledge{"doc": knowledge}, idx, false)
			if disabled || manual {
				if len(got) != 0 {
					t.Fatal("stale indexes and enrichment must not return disabled document content")
				}
			} else if len(got) != 2 {
				t.Fatal("enabled document must remain retrievable through primary and enrichment paths")
			}
		}
	}
}

func TestIsSearchableChunkSkipsUnsynchronizedEdits(t *testing.T) {
	service := &knowledgeBaseService{}
	for _, status := range []string{"processing", "failed"} {
		chunk := &types.Chunk{ChunkType: types.ChunkTypeText, IndexStatus: status, IsEnabled: true}
		if service.isSearchableChunk(chunk) {
			t.Fatalf("chunk with index status %q should not be searchable", status)
		}
	}
	for _, status := range []string{"", "ready"} {
		chunk := &types.Chunk{ChunkType: types.ChunkTypeText, IndexStatus: status, IsEnabled: true}
		if !service.isSearchableChunk(chunk) {
			t.Fatalf("chunk with index status %q should be searchable", status)
		}
	}
}

func TestIsSearchableChunkSkipsDisabledChunk(t *testing.T) {
	service := &knowledgeBaseService{}
	chunk := &types.Chunk{
		ChunkType:   types.ChunkTypeFAQ,
		IndexStatus: "ready",
		IsEnabled:   false,
	}
	if service.isSearchableChunk(chunk) {
		t.Fatal("disabled FAQ chunk should never be searchable")
	}
}
