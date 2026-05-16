# experiments/

Isolated experiment sandbox for evaluating self-hosted Liquid Foundation Models
(LFM) with **plain `transformers`, no quantization** (no Ollama / llama.cpp — we
want to see the raw, un-degraded model quality on our own hardware).

This area is intentionally separate from the docs-centric root of the repo.
It is built to scale to 100+ models without the folder structure collapsing.

## Naming convention

- One experiment per directory, named `NNN_slug`
  - `NNN` = 3-digit zero-padded sequential number (`001`, `002`, ... `137`)
  - `slug` = short kebab-case description (e.g. `local-lfm-web`)
- Example: `001_local-lfm-web`

## Model storage

- **All model weights live under `experiments/models/`** which is the shared
  `HF_HOME` cache. It is **git-ignored** — weights are never committed.
- **Do NOT create one folder per model.** Models are selected via a `MODEL_ID`
  parameter in each experiment's `config.py` / env. The same `models/` cache is
  reused across every experiment, so downloading 100+ models does not explode
  the tree.

## Where results go

| What | Where | Git tracked? |
|------|-------|--------------|
| Heavy raw logs, large measurement dumps | `experiments/runs/` | No (ignored) |
| Light comparison summary per experiment | `NNN_slug/results.md` | Yes |
| Experiment code | `NNN_slug/` | Yes |

Keep `results.md` small and human-readable (a table). Push verbose logs to
`runs/` so the repo stays light.

## Shared code (`common/`)

Deliberately thin helpers shared across experiments:

- `common/runtime.py` — `load_model(model_id)` and
  `generate(model, tokenizer, messages, **gen)` (no quantization, MPS-aware).
- `common/metrics.py` — measure tokens/s, generated token count, elapsed
  seconds, and process RSS memory (MB).

Do not over-engineer `common/`. It exists only to remove duplication.

## Per-experiment virtualenv

A single `.venv` lives at `experiments/.venv` (git-ignored). Activate it before
running any experiment:

```bash
source experiments/.venv/bin/activate
```

See each experiment's own `README.md` for run instructions.
