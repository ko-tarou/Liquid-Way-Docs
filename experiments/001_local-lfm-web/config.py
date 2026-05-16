"""Experiment 001 config.

IMPORTANT: HF_HOME is pinned to the in-repo (git-ignored) models cache and must
be set *before* transformers is imported anywhere, so this module does it at
import time. Always `import config` before importing transformers.
"""

import os
from pathlib import Path

# experiments/models — shared HF cache, git-ignored.
_MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
_MODELS_DIR.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("HF_HOME", str(_MODELS_DIR))

# Text-only LFM, no quantization, no multimodal.
MODEL_ID = "LiquidAI/LFM2.5-350M"

# Generation parameters (Liquid model-card recommended defaults).
GEN_PARAMS = {
    "do_sample": True,
    "temperature": 0.1,
    "top_k": 50,
    "repetition_penalty": 1.05,
    "max_new_tokens": 512,
}

# Web server.
HOST = "127.0.0.1"
PORT = 8000
