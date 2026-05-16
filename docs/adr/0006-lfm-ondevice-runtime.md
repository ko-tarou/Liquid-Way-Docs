# ADR-0006: LFM オンデバイス実行方式

- Status: Proposed
- Date: 2026-05-17
- Deciders: ko-tarou, Mako（参加意思確認中）

---

## Context

LiqMesh の AI（機能 6.1〜6.6）は **Layer 3 / server-only**、すなわち ADR-0003 で選出された昇格サーバー端末（または Ryzen AI PC）上でのみ動く（議事録 §9.3、liqmesh.md §4.1）。本番 Vision の Android 端末がサーバーに昇格した場合、その端末上で LFM をどう実行するかを決めます。

### 一次根拠: experiment 001 の実測（`experiments/001_local-lfm-web/results.md`）

本リポジトリで 2026-05-17 に実測済み。素の `transformers` 5.8.1 / `torch` 2.12.0、**量子化なし**、Apple Silicon MPS、bfloat16 で `LiquidAI/LFM2.5-350M` を自己ホスト:

| 指標 | 実測値 | 出典 |
|---|---|---|
| デコード速度 | **約 7〜8 tok/s**（52 トークン日本語生成で 8.18 tok/s、3 トークン英語で 7.31 tok/s） | results.md §Measurements |
| メモリ | **約 970〜985 MB RSS**（Liquid の "<1GB" 主張と一致） | results.md §Notes |
| 出力品質 | 温度 0.1 で日英ともに一貫・オントピック。`trust_remote_code` 不要 | results.md §Notes |
| 公称比 | Liquid 公称 313 tok/s（AMD CPU, llama.cpp）の**数十分の一** | results.md §Notes/Conclusion |

results.md の結論を引用すると「素の transformers（量子化なし）は **モデル品質を見るには十分**だが、レイテンシ要求のあるサービングには不適。将来実験で量子化／CPU ベースラインと比較すべき」。

→ 本 ADR の論拠: **素の transformers は質の検証には良いが、速度が公称の数十分の一。エッジ本番サービングには量子化された高速ランタイムが必須**であることが、推測でなく自前実測で裏付けられた。LiqMesh のサーバーは複数クライアントの推論を捌くため、7〜8 tok/s では機能 6.2/6.3/6.6 の体感が成立しない。

---

## Decision

**本番 Vision の Android サーバー端末上 LFM 実行は llama.cpp（GGUF, Android NDK ビルド）を第一候補とし、開発速度・保守性を優先する局面では LEAP SDK（Liquid 公式 Android, Kotlin）を採用する二段構えとする。素の transformers は本番サービング経路では使わない（評価・品質確認専用）。**

理由と ADR-0001 との関係:

- ADR-0001 は「**LFM 直ネイティブ実行を最優先**、最適化済みカーネル優先、ダメなら Lemonade SDK / FastFlowLM へフォールバック」と決めている。本 ADR はこれを **追認しつつ補正**する: ADR-0001 は実行先を Ryzen AI PC 中心に想定していたが、本番 Vision では**昇格した Android 端末**も実行先になる。Android 上での「直ネイティブ最適化済みカーネル」の具体は **llama.cpp（量子化 GGUF）** が現実解であり、ここが ADR-0001 への補正点。
- **llama.cpp（第一候補）**: GGUF 量子化で Android 実機 10〜20 tok/s（1〜3B Q4、Pixel 9 Pro / Galaxy S24 クラス、出典下記）。experiment 001 の素 transformers 7〜8 tok/s より明確に上で、サーバー用途の最低ラインを満たす。LFM2.5（テキスト）・**LFM2.5-VL（機能 6.1）も GGUF + llama.cpp で multimodal 推論に公式対応**（Liquid docs / Maxime Labonne, 出典下記）。NDK で C/C++ をクロスコンパイルし JNI で Kotlin から叩く＝ ADR-0001 の「直」方針に最も忠実。GMS 非依存で ADR-0002 の方針とも一貫。
- **LEAP SDK（補助・保守性優先時）**: Liquid 公式の Edge SDK、Android は Kotlin で **production-ready（公式に "well-tested and production-ready"、iOS は testing）**。モデルのライフサイクル管理（バックグラウンド時の解放）を内蔵しモバイルのメモリ肥大を防ぐ。全 LFM2.5（Base/Instruct/JP/VL/Audio）が LEAP 配布対象。実装速度・保守性を優先する場合や 12h MVP では LEAP を使う。

選定方針: **品質確認は transformers（experiment 系）、本番サーバー実行は llama.cpp（直・最速）、開発速度重視は LEAP**。三者は競合でなく役割分担。

---

## Alternatives Considered

| 案 | 長所 | 短所 | Android 制約 | 却下／位置づけ理由 |
|---|---|---|---|---|
| **llama.cpp (GGUF, NDK)（第一候補）** | 量子化で実機 10〜20 tok/s。LFM2.5 / VL とも GGUF 公式対応。ADR-0001「直」方針に最忠実。GMS 非依存 | NDK クロスビルド・JNI 実装コスト。GPU 加速は一部 SoC のみ | API/NDK ビルド、端末ごとに最適化要 | 採用（第一候補） |
| **LEAP SDK（Liquid 公式 Android）** | Kotlin、Android production-ready。ライフサイクル/メモリ管理内蔵。全 LFM2.5 配布対象 | 実行詳細がSDK内に隠蔽され低レベル最適化の自由度低 | Android 本番 Ready（iOS は testing、議事録 §5 と一致） | 採用（保守性・開発速度優先時／12h MVP） |
| ONNX Runtime Mobile | 量子化対応、クロスプラットフォーム成熟 | LFM2.5 系の VL/最新版対応の確証が薄い（**要検証**）。Liquid 一次サポートは GGUF/LEAP 優先 | NNAPI/実行プロバイダ選定が要 | 不採用（LFM 一次サポート経路でない。VL 対応未確認） |
| MediaPipe LLM Inference | Google 製、Android 統合容易 | 対応モデルが Google 寄り。LFM2.5 公式サポート確認できず（**要検証**）。GMS/依存懸念 | デバイス要件あり | 不採用（LFM 公式サポート外。ADR-0002 の GMS 非依存方針とも噛み合わない） |
| 素の transformers / PyTorch | 量子化不要で品質をそのまま観測可。実装容易 | **実測 7〜8 tok/s（公称の数十分の一）**。サービング不適（experiment 001 実証） | モバイルランタイムとして非現実的 | 本番サービングは不採用。**品質検証専用**として experiments で継続利用 |

---

## Consequences

### 正の帰結

- 「速度が必要な本番経路は量子化ランタイム」という判断を**自前実測（experiment 001）で根拠化**でき、推測でない設計になっている。
- llama.cpp が LFM2.5-VL も GGUF で扱えるため、機能 6.1（VL キャプション）の実行経路が単一ランタイムで閉じる。
- ADR-0001 の「直ネイティブ優先」を Android 文脈で具体化（= llama.cpp）し、フォールバック思想（LEAP / Lemonade / FastFlowLM）とも矛盾しない。

### 負の帰結・受容するトレードオフ

- llama.cpp は NDK ビルド／JNI／端末別チューニングのコストが大きい。これを受容し、開発速度が要る局面は LEAP に逃がす二段構え。
- Android 実機 10〜20 tok/s は出典が一般的フラッグシップ機の値で、**LFM2.5 特定モデル × 提供端末での実測は未取得（要検証）**。experiment 002 として GGUF 量子化＋llama.cpp の Android/CPU 実測を行うべき（results.md の結論が示唆する次実験）。
- ADR-0001 のフォールバック先 FastFlowLM は NPU 特化で Ryzen AI PC 向け。Android 端末側には適用されない（実行先で使い分け、と本 ADR で明確化）。

### フォローアップ

- experiment 002: LFM2.5-350M / 1.2B-JP / VL を GGUF 量子化し、llama.cpp で CPU / Android 実機スループットを実測（experiment 001 の Conclusion が予告した比較）。
- ADR-0005: 確定したサーバー実機スループットでキュー上限を再調整。
- 要検証: LFM2.5 特定モデル × 提供 Android 端末での llama.cpp 実測 tok/s、VL の GGUF 量子化品質劣化、LEAP の VL 対応詳細。

---

## 参考（事実確認の出典）

- 一次実測: `experiments/001_local-lfm-web/results.md`（本リポジトリ、2026-05-17）
- LEAP SDK Android production-ready: <https://leap.liquid.ai/> / <https://docs.liquid.ai/deployment/on-device/android/android-quick-start-guide>
- llama.cpp Android 性能（1–3B Q4 で 10–20 tok/s）: <https://github.com/ggml-org/llama.cpp/discussions/14356> / <https://github.com/ggml-org/llama.cpp/blob/master/docs/android.md>
- LFM2-VL GGUF + llama.cpp 公式対応: <https://docs.liquid.ai/deployment/on-device/llama-cpp> / <https://www.liquid.ai/blog/introducing-lfm2-5-the-next-generation-of-on-device-ai>

記録: Claude Code (Opus 4.7) / 2026-05-17
