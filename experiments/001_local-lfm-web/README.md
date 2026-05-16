# 001_local-lfm-web

## Purpose

Investigate the **raw AI quality and cost of self-hosting** a small Liquid
Foundation Model when run with **plain `transformers`, no quantization**
(no Ollama / llama.cpp — we want un-degraded quality, not peak speed).
Exposes a minimal localhost web chat plus per-request metrics (tok/s,
generated tokens, elapsed, RSS memory).

## Target model

- `LiquidAI/LFM2.5-350M` — text-only, no multimodal, no quantization.
- Selected via `MODEL_ID` in `config.py`. To try another model later, change
  that one constant; the shared `experiments/models/` cache is reused.

## Files

| File | Role |
|------|------|
| `config.py` | `MODEL_ID`, generation params; pins `HF_HOME` to `experiments/models/` before any transformers import |
| `app.py` | FastAPI: `GET /` minimal chat HTML, `POST /api/chat` → `{text, metrics}` |
| `smoke_test.py` | Framework-free: load → 1 generation → assert non-empty + metrics |
| `requirements.txt` | Pinned deps (fastapi, uvicorn, torch, transformers, accelerate) |
| `results.md` | Recorded measurements |

## Setup (one time)

```bash
python3 -m venv experiments/.venv
source experiments/.venv/bin/activate
pip install -r experiments/001_local-lfm-web/requirements.txt
```

## Verify it works

```bash
source experiments/.venv/bin/activate
python experiments/001_local-lfm-web/smoke_test.py
```

Expected: model loads (device/dtype printed), a non-empty response, a metrics
line, and `[smoke] PASS`. The first run downloads the model into
`experiments/models/` (git-ignored).

## Run the web app

```bash
source experiments/.venv/bin/activate && python experiments/001_local-lfm-web/app.py
```

Then open **http://127.0.0.1:8000** in a browser. Type a message and send;
the reply plus `tok/s · tokens · elapsed · RSS · device/dtype` is shown.
Port `8000` is used (configurable via `PORT` in `config.py`). Stop with
`Ctrl+C`.

Quick API check from another terminal:

```bash
curl -s -X POST http://127.0.0.1:8000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "Hello"}'
```

## Conclusion

See `results.md`. Summary: LFM2.5-350M self-hosts un-quantized on Apple Silicon
in ~1 GB RAM with coherent multilingual output; MPS raw-PyTorch decode is
~7–8 tok/s (evaluation-grade, not latency-optimized — quantization avoided on
purpose).
