"""Framework-free smoke test: load model -> 1 generation -> assert non-empty.

Run:
    source experiments/.venv/bin/activate
    python experiments/001_local-lfm-web/smoke_test.py
"""

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent.parent))

import config  # noqa: E402  (sets HF_HOME before transformers import)

from experiments.common.runtime import generate, load_model  # noqa: E402

PROMPT = "日本語で簡潔に自己紹介してください。"


def main() -> int:
    print(f"[smoke] loading {config.MODEL_ID} (no quantization)...")
    model, tokenizer = load_model(config.MODEL_ID)
    print(f"[smoke] device={model._exp_device} dtype={model._exp_dtype}")

    print(f"[smoke] prompt: {PROMPT}")
    text, metrics = generate(
        model,
        tokenizer,
        [{"role": "user", "content": PROMPT}],
        **config.GEN_PARAMS,
    )

    print(f"[smoke] response: {text}")
    print(f"[smoke] metrics: {metrics}")

    assert text.strip(), "model returned an empty response"
    assert metrics["generated_tokens"] > 0, "no tokens generated"
    assert metrics["tok_per_s"] > 0, "tok/s not measured"
    assert metrics["rss_mb"] > 0, "RSS memory not measured"

    print("[smoke] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
