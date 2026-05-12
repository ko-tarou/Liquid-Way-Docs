# Hack the Liquid WAY — 応募フォーム下書き

- 作成日: 2026-05-12
- 応募締切: 2026-05-29
- 対象: <https://luma.com/7fjlam5k>

このドキュメントは応募フォーム提出前のドラフトです。日本語版と英語版の両方を用意しています。Luma 経由の応募項目は最終確認後に追記してください。

---

## 0. 提出前のチェックリスト（要記入箇所）

| 項目 | 状態 |
|---|---|
| チーム名（正式） | **TBD** — 仮称: `LiqMesh Team` |
| Mako さんの氏名（実名） | **TBD** |
| Mako さんのメールアドレス | **TBD** |
| Mako さんのスキル・経歴 | **TBD** |
| Mako さんの GitHub / 連絡先 | **TBD** |
| 応募フォームの公式項目一覧（Luma の実フォーム） | **要最終確認**（本ドラフトは一般的な項目を想定） |

---

# 日本語版

## 1. チーム情報

| 項目 | 内容 |
|---|---|
| チーム名 | LiqMesh Team（仮） |
| 人数 | 2 名 |
| 代表者 | ko-tarou（akokoa1221@gmail.com） |
| メンバー 2 | TBD（Mako さん、参加意思最終確認中） |

## 2. メンバースキル概要

### ko-tarou

- バックエンド・フロントエンドの実装経験
- LFM2.5-350M を用いた Python Socket.IO ベースの早押しクイズアプリを実装した経験あり
- プロンプトのみで実用的なコード生成ができることを実機で確認済み

### TBD（Mako さん）

- スキル詳細は最終確認後に追記

---

## 3. アイデア概要（200 字版）

**LiqMesh** は、災害時に通信インフラが崩壊した状況下でも、被災者のスマートフォン同士が P2P でメッシュネットワークを組み、安定電源を確保した端末が動的に LFM サーバーとなって、被害情報の収集・統合・優先度判定・配信を **完全オフライン** で行うアプリです。LFM2.5 のテキスト・VL を端末上で実行することで、クラウド到達不可な災害現場でも被災者の情報主権を守りながら救援を加速します。

## 4. アイデア概要（400 字版）

**LiqMesh** は、災害時に通信インフラが崩壊した状況でも被災者間で情報共有を成立させる、P2P メッシュ × 動的 LFM サーバー型のアプリです。

被災者のスマートフォン同士が Wi-Fi Direct / BLE を用いて自律的にメッシュを構築し、その中で安定電源を確保した端末が **動的に「LFM サーバー」役** に昇格します。サーバー端末は、周囲のクライアントから集まる写真・テキスト・音声を LFM2.5（VL / Text）で処理し、状況キャプション生成・重複統合・矛盾解消・優先度判定（トリアージ）・自然言語クエリ応答を **完全オフライン** で実行します。

通信が回復した瞬間に差分のみクラウドへ同期する設計のため、データ主権と低レイテンシ、省電力を同時に満たします。クラウド到達不可・個人情報を晒せない・低消費電力が必須──この三条件下では **小型・高速 LFM が唯一解** であり、本企画は LFM の存在意義を最も鋭く可視化するユースケースです。

## 5. アイデア概要（1000 字版）

**LiqMesh** は、災害時に通信インフラが崩壊した状況下でも、被災者同士が情報を共有し合えるようにする、**P2P メッシュ × 動的 LFM サーバー** 型のアプリです。

### 解く課題

大規模災害が発生すると、基地局や固定回線は数日にわたって機能不全に陥り、クラウド前提のアプリ・SaaS はほぼ全てが停止します。一方、被災者のスマートフォン自体は手元に残り、電池や予備電源があれば稼働を続けられます。にもかかわらず、避難所でのリアルタイム情報共有は紙の張り紙や口頭に頼っており、情報が分散・重複・矛盾するまま、救援者にも届きません。

### LiqMesh の仕組み

被災者の端末は Wi-Fi Direct / BLE / 共有 AP を介して **P2P メッシュ** を組みます。メッシュ内で電源確保度・電池残量・性能スコアが最も高い端末が **「LFM サーバー」役に動的に昇格** し、LFM2.5 のテキスト・VL モデルをローカル実行します。

サーバー端末は次の 6 機能を提供します。

1. 写真 → 状況キャプション（VL）
2. テキスト重複統合・矛盾解消
3. 自然言語クエリ応答（例: 「水が足りていない避難所は？」）
4. Function Calling による救援アクション提案
5. 自動グルーピング（地理 × 種別）
6. 優先度判定（トリアージ）

通信が回復した瞬間、蓄積データの差分のみがクラウド／自治体ダッシュボードに送られます。

### なぜ LFM でしか成立しないか

クラウド到達不可・個人情報を集中させられない・端末の電力制約という三条件下で、これら 6 機能をすべて成立させるには、**端末上で動く小型・高速・マルチモーダルな LFM が唯一の解** です。LFM2.5-VL は 512×512 ネイティブで動作し、LFM2.5-1.2B-JP は <1GB で 70〜82 tok/s をスマートフォン上で達成します。これは LiqMesh のような P2P メッシュ運用に合致する性能プロファイルです。

### 期待される impact

LiqMesh は、災害時の情報の「断絶」「分散」「矛盾」「プライバシー懸念」を同時に解きます。前回 Gold 受賞作 SafeGuide は単機オフライン防災ガイダンスでしたが、LiqMesh は **多端末メッシュによる協調動作** という、SafeGuide では構造的に不可能だった領域に踏み込みます。災害領域における LFM の社会実装の最前線を提示できる企画と考えています。

---

## 6. 使用する LFM モデル

| 用途 | モデル |
|---|---|
| 写真 → 状況キャプション | **LFM2.5-VL-450M** または **1.6B** |
| テキスト統合・矛盾解消 | **LFM2.5-1.2B-JP** |
| 自然言語クエリ応答 | **LFM2.5-1.2B-JP** または **1.2B-Thinking** |
| 優先度判定（トリアージ） | **LFM2.5-1.2B-Thinking** |
| Function Calling | **LFM2.5** ※ 対応状況を要確認 |

## 7. 解決する課題

- 災害時の通信インフラ崩壊によるクラウド到達不可
- 避難所での情報の分散・重複・矛盾
- 個人情報をクラウドに集中させたくないというデータ主権の課題
- 端末電力制約下での AI 機能の継続稼働

## 8. なぜ LFM が必要か

クラウド到達不可・個人情報を晒せない・低消費電力という三条件を同時に満たす AI は、現状 **小型・高速・マルチモーダルな LFM** だけです。汎用 LLM をクラウドで動かす方式や、巨大モデルを端末で動かす方式はいずれも要件を満たせません。LiqMesh は LFM の存在意義を最も鋭く可視化するアプリケーションです。

## 9. 期待される impact

- 災害時の情報共有における「インフラ完全ゼロでも成立する」という新しい標準の提示
- 自治体・防災 NPO・国際赤十字などへのリファレンス実装としての展開可能性
- LFM の社会実装事例として、防災テックの新領域を切り開く

---

# English Version

## 1. Team

| Field | Value |
|---|---|
| Team name | LiqMesh Team (tentative) |
| Size | 2 |
| Lead | ko-tarou (akokoa1221@gmail.com) |
| Member 2 | TBD (Mako — pending confirmation) |

## 2. Member Skills

### ko-tarou

- Full-stack engineer with both backend and frontend implementation experience.
- Previously built a real-time buzz-in quiz app on top of LFM2.5-350M with Python and Socket.IO, and verified that prompt-only usage produces production-quality code on-device.

### TBD (Mako)

- Skills to be added after confirmation.

---

## 3. Short Pitch (≈ 50 words)

**LiqMesh** is a P2P mesh app that keeps disaster victims connected when cellular networks collapse. Phones form an ad-hoc mesh, the device with the most stable power dynamically promotes itself to an on-device LFM server, and LFM2.5 processes photos, text, and audio entirely offline — preserving data sovereignty while accelerating relief.

## 4. Medium Pitch (≈ 120 words)

**LiqMesh** turns a disaster site into a self-organizing intelligence network. When base stations go down, victims' phones form a P2P mesh over Wi-Fi Direct / BLE. The device with the strongest power profile is dynamically elected as an **on-device LFM server**, running LFM2.5-VL and LFM2.5-1.2B-JP locally to (1) caption disaster photos, (2) deduplicate and reconcile conflicting reports, (3) answer natural-language queries, (4) emit structured relief actions via function calling, (5) cluster reports by geography and type, and (6) triage by priority. When connectivity returns, only the deltas sync to the cloud. No cloud, no data leakage, no idle silicon. LFM is the only class of model that can satisfy all three of these constraints simultaneously.

## 5. Long Pitch (≈ 350 words)

**LiqMesh** is a peer-to-peer disaster response app powered by on-device Liquid Foundation Models.

When a large-scale disaster strikes, cellular and fixed-line infrastructure go down for days. Cloud-based apps stop working. Victims, however, still have their phones in hand. Today, information sharing inside evacuation shelters falls back to paper notices and word of mouth — fragmented, contradictory, and invisible to rescue workers.

LiqMesh closes that gap. Phones form an ad-hoc P2P mesh over Wi-Fi Direct / BLE / shared APs. Within the mesh, the device with the highest "server fitness score" — based on power source, battery level, and compute capability — is dynamically promoted to act as a local **LFM inference server**. Other phones submit photos, text, and audio reports to it.

The server-elected device runs six core AI functions, all locally on LFM2.5:

1. Photo-to-caption with LFM2.5-VL
2. Deduplication and contradiction resolution
3. Natural-language query answering
4. Function calling for relief actions
5. Geographic and categorical clustering
6. Triage priority scoring

Once upstream connectivity returns — via Starlink, satellite, or restored MNOs — only the deltas are synced to the cloud, where civic dashboards can consume them.

LFM is uniquely suited to this. Cloud LLMs are unreachable when the network is down. Large on-device models drain batteries and cannot run on shelter-grade hardware. LFM2.5-VL fits in well under a gigabyte and runs natively at 512x512. LFM2.5-1.2B-JP achieves 70–82 tok/s on a phone. These are the exact characteristics that a mesh-based disaster response system needs.

LiqMesh differentiates from prior Hack the Liquid winners (e.g., SafeGuide's single-device offline guidance) by introducing **multi-device cooperation**. It is a category SafeGuide structurally could not enter. We believe LiqMesh demonstrates the social-implementation frontier of LFM in disaster tech.

---

## 6. LFM Models Used

| Function | Model |
|---|---|
| Photo captioning | **LFM2.5-VL-450M** or **1.6B** |
| Text deduplication / reconciliation | **LFM2.5-1.2B-JP** |
| Natural-language Q&A | **LFM2.5-1.2B-JP** or **1.2B-Thinking** |
| Triage scoring | **LFM2.5-1.2B-Thinking** |
| Function calling | **LFM2.5** (support to be confirmed) |

## 7. Problem Statement

- Cloud is unreachable when cellular and fixed-line infrastructure collapse during disasters.
- Information inside shelters is fragmented, duplicated, and contradictory.
- Centralizing victims' personal data in the cloud raises serious data-sovereignty concerns.
- Battery-constrained devices must keep AI features running for days.

## 8. Why LFM Is Necessary

Only a class of models that is simultaneously small, fast, and multimodal can satisfy all three constraints — cloud-unreachable, no centralized PII, and tight power budgets. Cloud LLMs cannot run when the network is down. Large on-device models drain the battery and exceed device memory. LFM2.5 sits precisely in the only viable corner of this design space, which is why LiqMesh is one of the sharpest demonstrations of what LFM uniquely enables.

## 9. Expected Impact

- Establishes a new standard for disaster information sharing: "no infrastructure required."
- Provides a reference implementation for municipalities, disaster NGOs, and international relief organizations.
- Opens a new frontier for socially-deployed LFM applications in disaster tech.

---

記録: Claude Code (Opus 4.7) / 2026-05-12
