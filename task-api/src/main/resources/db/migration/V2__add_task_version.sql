-- Optimistic-lock version column. Used by the worker's retry sweeper to
-- atomically claim tasks stuck in RETRY_SCHEDULED so a task is never recovered
-- (re-queued) by two workers at once.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;

-- Speeds up the sweeper's scan for overdue retries.
CREATE INDEX IF NOT EXISTS idx_tasks_status_next_retry
    ON tasks (status, next_retry_at)
    WHERE next_retry_at IS NOT NULL;
