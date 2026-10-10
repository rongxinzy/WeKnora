-- Separate explicit user intent from the transient disabled state used while parsing.
ALTER TABLE knowledges
    ADD COLUMN IF NOT EXISTS manual_disabled BOOLEAN NOT NULL DEFAULT FALSE;

-- A completed row that was already disabled represents a durable user choice;
-- preserve it when separating that choice from temporary parse state.
UPDATE knowledges
SET manual_disabled = TRUE
WHERE parse_status = 'completed' AND enable_status = 'disabled';
