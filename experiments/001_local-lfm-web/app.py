"""FastAPI localhost chat app for self-hosted LFM2.5-350M (no quantization).

Run:
    source experiments/.venv/bin/activate
    python experiments/001_local-lfm-web/app.py
Then open http://127.0.0.1:8000 in a browser.
"""

import sys
from pathlib import Path

# Make sibling `config` and the `experiments` package importable when run
# directly as a script.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent.parent))

import config  # noqa: E402  (sets HF_HOME before transformers import)
import uvicorn  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.responses import HTMLResponse, JSONResponse  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from experiments.common.runtime import generate, load_model  # noqa: E402

app = FastAPI(title="local-lfm-web")
_STATE: dict = {}


class ChatRequest(BaseModel):
    message: str


INDEX_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>local LFM2.5-350M chat</title>
<style>
  body { font-family: system-ui, sans-serif; max-width: 720px; margin: 2rem auto;
         padding: 0 1rem; color: #1a1a1a; }
  h1 { font-size: 1.1rem; }
  #log { border: 1px solid #ddd; border-radius: 8px; padding: 1rem;
         min-height: 200px; white-space: pre-wrap; }
  .u { color: #2563eb; } .a { color: #047857; }
  form { display: flex; gap: .5rem; margin-top: 1rem; }
  input { flex: 1; padding: .6rem; border: 1px solid #ccc; border-radius: 6px; }
  button { padding: .6rem 1rem; border: 0; border-radius: 6px;
           background: #2563eb; color: #fff; cursor: pointer; }
  button:disabled { opacity: .5; cursor: progress; }
  #stats { color: #666; font-size: .8rem; margin-top: .5rem; }
</style>
</head>
<body>
<h1>local LFM2.5-350M &mdash; plain transformers, no quantization</h1>
<div id="log"></div>
<div id="stats"></div>
<form id="f">
  <input id="m" placeholder="Ask something..." autocomplete="off" required>
  <button id="b" type="submit">Send</button>
</form>
<script>
const log = document.getElementById('log');
const stats = document.getElementById('stats');
const f = document.getElementById('f');
const m = document.getElementById('m');
const b = document.getElementById('b');
f.addEventListener('submit', async (e) => {
  e.preventDefault();
  const msg = m.value.trim();
  if (!msg) return;
  log.innerHTML += `<div class="u">You: ${msg}</div>`;
  m.value = ''; b.disabled = true; stats.textContent = 'generating...';
  try {
    const r = await fetch('/api/chat', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({message: msg})
    });
    const d = await r.json();
    log.innerHTML += `<div class="a">LFM: ${d.text}</div>`;
    const x = d.metrics;
    stats.textContent =
      `${x.tok_per_s} tok/s · ${x.generated_tokens} tokens · ` +
      `${x.elapsed_s}s · RSS ${x.rss_mb} MB · ${x.device}/${x.dtype}`;
  } catch (err) {
    log.innerHTML += `<div>error: ${err}</div>`;
    stats.textContent = '';
  } finally {
    b.disabled = false;
    log.scrollTop = log.scrollHeight;
  }
});
</script>
</body>
</html>"""


@app.on_event("startup")
def _startup():
    print(f"Loading {config.MODEL_ID} (no quantization)...")
    model, tokenizer = load_model(config.MODEL_ID)
    _STATE["model"] = model
    _STATE["tokenizer"] = tokenizer
    print(f"Loaded on {model._exp_device} ({model._exp_dtype}).")


@app.get("/", response_class=HTMLResponse)
def index():
    return INDEX_HTML


@app.post("/api/chat")
def chat(req: ChatRequest):
    text, metrics = generate(
        _STATE["model"],
        _STATE["tokenizer"],
        [{"role": "user", "content": req.message}],
        **config.GEN_PARAMS,
    )
    return JSONResponse({"text": text, "metrics": metrics})


if __name__ == "__main__":
    uvicorn.run(app, host=config.HOST, port=config.PORT)
