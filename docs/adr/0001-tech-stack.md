# ADR-0001: LiqMesh 技術スタック選定

- Status: Accepted
- Date: 2026-05-12
- Deciders: ko-tarou, Mako（参加意思確認中）

---

## Context

「Hack the Liquid WAY」ハッカソン（2026-06-06 / 07）に向けて、LiqMesh（災害時 P2P メッシュ × 動的 LFM サーバー）の技術スタックを決定する必要があります。

制約条件は以下の通りです。

| 制約 | 内容 |
|---|---|
| ハッカソン時間 | 12 時間（Day1 6h + Day2 3h、デモ準備込み） |
| 必須技術 | LFM2.5 系モデル / Weights & Biases 計測 |
| デプロイ先 | AMD Ryzen AI PC（NUC または Strix Halo） |
| プラットフォーム | 審査員 / 来場者が実機で触れること |
| チーム規模 | 2 名（うち 1 名は参加意思確認中） |

加えて、LiqMesh は本来 1 ヶ月規模の案件であり、「本番 Vision」と「Hackathon 12 時間 MVP」のスコープを **明確に分離** する必要があります。

公式テーマの北極星は「パラメータあたりの性能と計算メモリの効率」であり、**電力効率と省メモリで殴る** ことが評価軸として重要です。

---

## Decision

### 本番 Vision（理想形）

| レイヤ | 採用技術 |
|---|---|
| Android クライアント | **Native Kotlin + Jetpack Compose** |
| サーバー | **Go** |
| LFM ランタイム | **LFM 直ネイティブ実行**（最適化済みカーネル優先） |
| iOS / PC クライアント | Stretch goal（本番 MVP では非対応） |
| 計測 | **Weights & Biases**（エッジ計測） |

### Hackathon 12 時間 MVP

- 最終確定は **応募採択後の事前準備期間（2026-06-01 〜 06-05）に判断** する
- 候補は以下:
  1. Native Kotlin + Go（本番と同じ。ストーリー一貫だが工数大）
  2. **PWA + Python + Lemonade SDK**（OpenAI 互換 API、組立速度最優先）
  3. ハイブリッド（PWA クライアント + Go サーバー）

### 対象プラットフォーム

- **Android 一本** で MVP を仕上げる
- iOS / PC は stretch goal とし、12 時間 MVP では着手しない

### LFM 実行方式

- **直ネイティブ実行を最優先で試行** する
- 動かない / 統合が重い場合は **Lemonade SDK（OpenAI 互換 API）** にフォールバック
- NPU 効率を最大化したい場合は **FastFlowLM** を試す

---

## Alternatives Considered

| 候補 | 評価 | 不採用理由 |
|---|---|---|
| **React Native / Flutter** クライアント | △ | 電力効率で Native Kotlin に劣後。公式テーマ「省電力」訴求が弱まる |
| **Python サーバー（FastAPI 等）** | △ | 性能で Go に劣後。並行処理・クロスコンパイル容易性で Go の方が Ryzen AI PC との親和性が高い。ただし MVP 用途では選択肢に残す |
| **VLLM / 生 C++ ランタイム** | △ | 学習コスト大、12 時間で組み込むには重い。直ネイティブ実行で代替不可だった場合の再評価対象 |
| **iOS / SwiftUI 先行** | × | LEAP SDK は iOS が testing 段階。Android が本番 Ready。デモの安定性で Android が圧倒的に有利 |
| **クラウド + オンデバイス ハイブリッド** | × | LiqMesh の核は「完全オフライン成立」。ハイブリッドは差別化を弱める |
| **BLE Mesh ライブラリ採用** | × | レンジ・スループット不足。Wi-Fi Direct + Ryzen AI PC を AP にする構成で観客デモは十分成立 |

---

## Consequences

### 正の影響

- **電力効率最適**（Native Kotlin + 直ネイティブ LFM）→ 公式テーマ「リソース効率」軸で強くアピールできる
- **性能最大**（Go サーバー + NPU 最適化）→ 観客デモで体感品質が高い
- **提供 Ryzen AI PC との親和性が高い**（Linux / Windows いずれでも Go バイナリは動く）
- **本番 Vision と MVP のスコープが明確に分離** されているため、12 時間内での意思決定が高速

### 負の影響

- **開発工数大**：Android Native + Go サーバーの両方を構築する必要があり、2 名で 12 時間は厳しい
- **MVP では一部簡略化が必須**：機能を 6 → 3〜4 に絞る Tier 分けが前提
- **LFM 直ネイティブ実行が動かないリスク**：フォールバック（Lemonade / FastFlowLM）が必須
- **iOS ユーザーが体験できない**：審査員に iPhone ユーザーがいた場合、デモは Android 端末を貸し出す対応が必要

### 後続意思決定

以下は別 ADR で記録する予定です。

- ADR-0002: P2P メッシュ通信方式（Wi-Fi Direct / BLE / ハイブリッド）
- ADR-0003: 動的 LFM サーバー選出アルゴリズム
- ADR-0004: オフライン蓄積 / クラウド同期の差分戦略
- ADR-0005: W&B エッジ計測の組み込み方式

---

記録: Claude Code (Opus 4.7) / 2026-05-12
