-- Mirrors versioned migration 000104_knowledge_lifecycle_images.
ALTER TABLE knowledges ADD COLUMN manual_disabled BOOLEAN NOT NULL DEFAULT FALSE;
UPDATE knowledges
SET manual_disabled = TRUE
WHERE parse_status = 'completed' AND enable_status = 'disabled';
