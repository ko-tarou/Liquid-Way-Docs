# Experiment 001 — results

Self-hosted **LFM2.5-350M**, plain `transformers`, **no quantization**.
Measured on macOS 26.2, Apple Silicon (arm64), MPS backend.

## Environment

| Item | Value |
|------|-------|
| Model | `LiquidAI/LFM2.5-350M` (text-only, no multimodal) |
| Loader | `transformers` 5.8.1, `torch` 2.12.0, no quantization |
| Device | MPS (Apple Silicon) |
| dtype | bfloat16 |
| `trust_remote_code` | Not required |
| Generation | do_sample, temp 0.1, top_k 50, rep_penalty 1.05, max_new_tokens 512 |

## Measurements

| Prompt | Response (excerpt) | Gen tokens | tok/s | Elapsed | RSS MB |
|--------|--------------------|-----------:|------:|--------:|-------:|
| `日本語で簡潔に自己紹介してください。` | 私はAIアシスタントです。あなたの質問に答えるために…自然とテクノロジーを融合させた新しい方法で… | 52 | 8.18 | 6.35 s | 970 |
| `What is the capital of Japan? Answer in one short sentence.` (via `POST /api/chat`) | `Tokyo` | 3 | 7.31 | 0.41 s | 985 |

## Notes / observations

- Loads and runs **un-quantized** on Apple Silicon via MPS + bfloat16. Memory
  footprint stays around **~1 GB RSS** for the 350M model — matches Liquid's
  "<1GB" claim.
- Decode throughput on MPS here is **~7–8 tok/s** for short outputs. This is
  far below Liquid's quoted 313 tok/s (AMD CPU, llama.cpp) — expected, since we
  deliberately avoid quantization and run raw PyTorch on MPS to observe *raw
  model quality*, not peak speed.
- Output quality at temp 0.1 is coherent and on-topic in both Japanese and
  English. No `trust_remote_code` needed; the architecture is supported
  natively in current `transformers`.
- tok/s is sensitive to output length (short 3-token reply skews the rate);
  the 52-token Japanese run is the more representative throughput sample.

## Conclusion

Self-hosting LFM2.5-350M with plain transformers (no quantization) is viable on
a single Apple Silicon machine: ~1 GB memory, coherent multilingual output.
Throughput on MPS raw PyTorch (~8 tok/s) is acceptable for evaluation but not
for latency-sensitive serving — a future experiment can compare against
larger LFM variants and against CPU/quantized baselines.
