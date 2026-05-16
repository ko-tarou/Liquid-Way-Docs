"""Plain-transformers model loading and generation. No quantization.

Device preference: MPS (Apple Silicon) > CPU.
dtype: bfloat16/float16 on MPS, float32 on CPU.
"""

from __future__ import annotations

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from .metrics import build_metrics, timer


def _pick_device() -> str:
    if torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def load_model(model_id: str):
    """Load ``model_id`` with full-precision weights (no quantization).

    Returns ``(model, tokenizer)``. The model is placed on the best device.
    """
    device = _pick_device()
    dtype = torch.bfloat16 if device == "mps" else torch.float32

    tokenizer = AutoTokenizer.from_pretrained(model_id)
    model = AutoModelForCausalLM.from_pretrained(model_id, dtype=dtype)
    model.to(device)
    model.eval()
    # Stash for downstream code / logging.
    model._exp_device = device
    model._exp_dtype = str(dtype).replace("torch.", "")
    return model, tokenizer


def generate(model, tokenizer, messages, **gen):
    """Generate a reply for chat ``messages`` and return ``(text, metrics)``.

    ``messages`` is a list of ``{"role", "content"}`` dicts. The model's chat
    template is applied when present. ``gen`` overrides generation kwargs.
    """
    gen_kwargs = {
        "do_sample": True,
        "temperature": 0.1,
        "top_k": 50,
        "repetition_penalty": 1.05,
        "max_new_tokens": 512,
    }
    gen_kwargs.update(gen)

    if tokenizer.chat_template:
        encoded = tokenizer.apply_chat_template(
            messages,
            add_generation_prompt=True,
            return_tensors="pt",
            tokenize=True,
            return_dict=True,
        )
        input_ids = encoded["input_ids"]
    else:  # fallback: concatenate content
        prompt = "\n".join(m["content"] for m in messages)
        input_ids = tokenizer(prompt, return_tensors="pt").input_ids
    input_ids = input_ids.to(model.device)
    prompt_len = input_ids.shape[-1]

    with timer() as elapsed, torch.no_grad():
        output = model.generate(input_ids, **gen_kwargs)
    elapsed_s = elapsed()

    new_tokens = output[0][prompt_len:]
    text = tokenizer.decode(new_tokens, skip_special_tokens=True).strip()
    metrics = build_metrics(int(new_tokens.shape[-1]), elapsed_s)
    metrics["device"] = getattr(model, "_exp_device", "?")
    metrics["dtype"] = getattr(model, "_exp_dtype", "?")
    return text, metrics
