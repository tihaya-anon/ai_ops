CREATE TABLE IF NOT EXISTS event_candidates_v1 (
  candidate_id TEXT PRIMARY KEY,
  source_event_id TEXT NOT NULL,
  observed_at TIMESTAMPTZ NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_event_candidates_v1_observed_at
  ON event_candidates_v1 (observed_at DESC);
