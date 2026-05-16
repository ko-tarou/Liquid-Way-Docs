# ADR-0002: メッシュ転送方式の選定

- Status: Proposed
- Date: 2026-05-17
- Deciders: ko-tarou, Mako（参加意思確認中）

---

## Context

LiqMesh の Layer 1（P2P Mesh Transport）で、被災者端末同士をインターネット完全不要で接続する転送方式を決める必要があります（議事録 §9.3、liqmesh.md §4.1）。要件は以下の通りです。

| # | 要件 | 根拠 |
|---|---|---|
| R1 | 多数端末（数十台規模）が同時に参加できる | 避難所での多者協調が存在意義（liqmesh.md §2-3） |
| R2 | マルチホップ中継（端末間ホップ転送）が可能 | 議事録 §9.3「端末発見・ホップ転送」 |
| R3 | 写真サイズ（数百 KB〜数 MB）のペイロードを運べる | 機能 6.1 VL キャプションの入力が写真 |
| R4 | インターネット・基地局が完全不要 | LiqMesh の核（liqmesh.md §2 課題1） |
| R5 | Android 実機（本番 Vision は Native Kotlin、ADR-0001） |  |
| R6 | GMS（Google Play Services）非依存が望ましい | 災害時は Play Store / GMS 更新も到達不可になりうる |

ADR-0001 では BLE Mesh ライブラリを「レンジ・スループット不足」として既に却下済みであり、本 ADR はその判断を事実で裏取りした上で、Wi-Fi 系を含めた候補から本番 Vision の推奨を 1 つ決めます。なお ADR-0001 のデモ構成（Ryzen AI PC = Wi-Fi AP の星型）と、本 ADR が定義する本番 Vision の P2P メッシュは別物であり、両立します（デモは星型に縮退、本番はフルメッシュ）。

---

## Decision

**本番 Vision の Layer 1 転送方式は Wi-Fi Direct（Wi-Fi P2P, `android.net.wifi.p2p`）を主方式として採用する。** Group Owner（GO）が DHCP 兼ルーティングノードとして自然に「サーバー候補」になる構造を、ADR-0003 の動的サーバー選出と接続して使う。

根拠:

- **帯域**: Wi-Fi Direct 認証デバイスは典型 Wi-Fi 速度（最大 250 Mbps クラス）を出せる。写真ペイロード（R3）に十分（出典: Android Developers / devopedia, 下記）。BLE Mesh は実効数 kbps〜80 kbps・アプリ実ペイロード 11〜15 byte/メッセージで、写真転送には原理的に不適（出典: Bluetooth Mesh 解説, 下記）。これで ADR-0001 の BLE Mesh 却下判断を**追認**する。
- **GMS 非依存（R6）**: Wi-Fi Direct は AOSP のフレームワーク API（`WifiP2pManager`）であり Google Play Services を要求しない。Nearby Connections は `com.google.android.gms:play-services-nearby` 依存で、災害時に GMS が劣化／未更新の端末で動作保証が弱い（出典: Google for Developers, 下記）。
- **GO = サーバー役の自然な対応**: Wi-Fi Direct グループは「1 GO + 複数 P2P クライアント」の星型で、GO が DHCP サーバとアドレス割当を持つ。LiqMesh の「安定電源端末が LFM サーバーに昇格」（議事録 §9.2）と構造が一致し、ADR-0003 のスコアで選ばれた端末を GO に固定する設計に落とせる。
- **マルチホップ（R2）**: Wi-Fi Direct 単体はグループ内星型までしか規定しない。**複数 GO グループをアプリ層のストア&フォワード中継でつなぐ**ことでマルチホップを実現する（GO 端末が隣接グループにレガシークライアントとして二重所属、またはメッセージを物理的に運ぶ端末経由のエピデミック転送）。これは ADR-0005 の優先度キューと同じメッセージ層で実装する。

**フォールバック / デモ縮退**: 12 時間 MVP では ADR-0001 通り「Ryzen AI PC = Wi-Fi AP」の星型インフラに縮退し、Wi-Fi Direct のマルチグループ中継は本番 Vision のスコープに置く（議事録 §11）。

---

## Alternatives Considered

| 案 | 長所 | 短所 | Android 制約 | 却下理由 |
|---|---|---|---|---|
| **Wi-Fi Direct（採用）** | 高帯域（〜250 Mbps級）で写真可。GMS 非依存。GO がサーバー役に自然対応 | 標準では 2 端末間ネゴ前提でマルチグループ中継はアプリ層自作。GO 選出の制御が OS 任せ | API 14+ の `WifiP2pManager`。GMS 不要。同時クライアント数は端末依存（標準が明確上限を規定せず） | — |
| Google Nearby Connections（P2P_CLUSTER） | M:N クラスタでメッシュ的トポロジを公式サポート。完全オフライン動作可。BT/Wi-Fi/BLE を自動選択 | **GMS 必須**（`play-services-nearby`、Play services 11.0+）。災害時 GMS 劣化リスク。CLUSTER は STAR より低帯域 | GMS 依存（R6 不適合）。レンジ約 100m | R6（GMS 非依存）に反する。災害時に GMS 自体が更新・到達不可になりうる前提と矛盾 |
| Wi-Fi Aware（NAN） | BT より高スループット・長距離。Android 12+ で responder が任意ピア受理でき複数 P2P リンクを 1 リクエストで張れる。インフラ不要 | **端末対応の断片化が深刻**。HW/ファーム次第で未対応、メーカー有効化任せ。Wi-Fi Direct/SoftAP/テザリング併用中は使えない場合あり | API 26+ だが `isAvailable()` が false の実機が多数（例: 一部 OnePlus/旧 Pixel）。可用性が動的に変化 | 観客の手持ち端末で確実に動く保証がない。デモ／実運用の再現性リスクが Wi-Fi Direct より高い |
| BLE Mesh | 超低電力。多ホップ・多ノードのトポロジを標準化 | **実効スループットが数 kbps〜80 kbps、アプリ実ペイロード 11〜15 byte/メッセージ**。画像転送は原理的に不可 | Android 標準 API なし（ベンダ SDK / 自作スタック）。GATT 越え実装が重い | R3（写真ペイロード）を満たせない。ADR-0001 の却下を事実で追認 |

---

## Consequences

### 正の帰結

- 写真（機能 6.1）を含むペイロードを実用帯域で運べる。ADR-0006 の VL 入力経路が成立。
- GMS 非依存により「インフラ完全ゼロ・端末だけで成立」（議事録 §9.5）という LiqMesh の差別化を技術的に裏付けられる。
- GO 構造が ADR-0003 の選出結果（サーバー端末）と直接マッピングでき、設計の一貫性が高い。

### 負の帰結・受容するトレードオフ

- **マルチホップ中継はアプリ層で自作が必要**（Wi-Fi Direct 標準範囲外）。実装コスト増を受容し、ADR-0004/0005 のメッセージ層に統合する。
- GO の選出・再選出は OS の P2P ネゴと自前ロジックの二層になり、競合制御が複雑（ADR-0003 で詳細化）。
- 同時接続クライアント数の上限が端末依存で、標準が明示上限を規定しない（**要検証**: 主要実機での実測が必要）。
- 12 時間 MVP では星型 AP に縮退するため、フルメッシュ動作の検証は本番 Vision フェーズに先送り。

### フォローアップ

- ADR-0003: GO 端末をどのスコアで選び、故障時にどう GO を切り替えるか。
- ADR-0005: マルチグループ中継時の優先度キューとストア&フォワード方針。
- 要検証タスク: 主要 Android 実機（Pixel / Galaxy）での Wi-Fi Direct 同時クライアント数とマルチグループ二重所属の可否を実測。

---

## 参考（事実確認の出典）

- Wi-Fi Direct（Android）: <https://developer.android.com/develop/connectivity/wifi/wifip2p> / 速度・GO 構造: <https://devopedia.org/wi-fi-direct>
- Nearby Connections（GMS 依存・トポロジ・レンジ）: <https://developers.google.com/nearby/connections/overview> / <https://developers.google.com/nearby/connections/strategies>
- Wi-Fi Aware（API 26+、断片化、`isAvailable()`）: <https://developer.android.com/develop/connectivity/wifi/wifi-aware> / <https://www.wi-fi.org/knowledge-center/faq/can-my-device-be-updated-to-support-wi-fi-aware>
- BLE Mesh スループット／ペイロード制約: <https://argenox.com/blog/10-reasons-why-ble-mesh-has-struggled-to-gain-traction> / <https://www.beaconzone.co.uk/blog/the-limitations-of-bluetooth-mesh/>

記録: Claude Code (Opus 4.7) / 2026-05-17
