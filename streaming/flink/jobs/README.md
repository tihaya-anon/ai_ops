# Flink jobs (speed layer)

Jobs (incremental; keep runnable locally):

- `news_normalize_job`: consume `news_events_raw_v1` -> normalize/dedup -> publish `news_events_v1`
