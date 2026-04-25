# Mock API (raw messages)

This is a **mock upstream provider** that returns intentionally messy ("dirty") raw messages, so the downstream Flink job can demonstrate cleaning/normalization.

## Run (local)

```bash
pip install -r serving/api/requirements.txt
uvicorn serving.api.app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Example

```bash
curl 'http://localhost:8000/v1/raw/news?limit=5&seed=42'
```

