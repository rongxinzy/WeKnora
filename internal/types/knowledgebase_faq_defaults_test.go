package types

import (
	"encoding/json"
	"testing"
)

func TestFAQDefaultsEnableIndexingWithMissingFAQConfig(t *testing.T) {
	kb := &KnowledgeBase{Type: KnowledgeBaseTypeFAQ}
	kb.EnsureDefaults()
	if kb.FAQConfig == nil || kb.FAQConfig.IndexMode != FAQIndexModeQuestionAnswer || kb.FAQConfig.QuestionIndexMode != FAQQuestionIndexModeCombined {
		t.Fatal("new FAQ base must have the supported default FAQ configuration")
	}
	if !kb.IsVectorEnabled() || !kb.IsKeywordEnabled() {
		t.Fatal("new FAQ base must remain searchable when FAQ configuration is omitted")
	}
	stored, err := kb.IndexingStrategy.Value()
	if err != nil {
		t.Fatal(err)
	}
	var reloaded IndexingStrategy
	if err := reloaded.Scan(stored); err != nil {
		t.Fatal(err)
	}
	if !reloaded.VectorEnabled || !reloaded.KeywordEnabled {
		t.Fatal("persisted FAQ indexing defaults must survive a database round trip")
	}
	encoded, err := json.Marshal(kb)
	if err != nil {
		t.Fatal(err)
	}
	var response struct {
		Capabilities KBCapabilities `json:"capabilities"`
	}
	if err := json.Unmarshal(encoded, &response); err != nil {
		t.Fatal(err)
	}
	if !response.Capabilities.FAQ || !response.Capabilities.Vector {
		t.Fatal("FAQ API capabilities must advertise actual default search support")
	}
}

func TestFAQDefaultsPreserveExplicitConfigurationAndIndexing(t *testing.T) {
	for _, config := range []*FAQConfig{nil, {}, {IndexMode: FAQIndexModeQuestionOnly, QuestionIndexMode: FAQQuestionIndexModeSeparate}} {
		kb := &KnowledgeBase{Type: KnowledgeBaseTypeFAQ, FAQConfig: config, IndexingStrategy: IndexingStrategy{KeywordEnabled: true}}
		kb.EnsureDefaults()
		if kb.IndexingStrategy.VectorEnabled || !kb.IndexingStrategy.KeywordEnabled {
			t.Fatal("explicit keyword-only indexing must remain unchanged")
		}
		if config != nil && config.IndexMode == FAQIndexModeQuestionOnly && kb.FAQConfig.QuestionIndexMode != FAQQuestionIndexModeSeparate {
			t.Fatal("explicit question-only/separate FAQ policy must remain unchanged")
		}
		before := *kb.FAQConfig
		kb.EnsureDefaults()
		if *kb.FAQConfig != before {
			t.Fatal("FAQ defaults must be idempotent")
		}
	}
}
