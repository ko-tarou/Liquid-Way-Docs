# LiqMesh — disaster-time P2P chat with on-device AI summaries

LiqMesh is a Kotlin + Jetpack Compose Android app for staying coordinated when
the internet is down. Devices on the **same Wi-Fi / phone hotspot** form a TCP
star network and chat peer-to-peer with no server, no account, and no cloud.
One device runs in **server (hub) mode** and additionally loads a Liquid
Foundation Model **on-device** via the [LEAP SDK](https://leap.liquid.ai/)
(LFM2-350M), so anyone can tap "状況まとめ" and the hub produces an offline,
on-device situational summary of the conversation and relays it to everyone.

The earlier LEAP smoke proof (`LfmEngine.kt`, verified end-to-end on a headless
arm64 emulator) is preserved verbatim and is the production inference path.

## Architecture (4 layers)

1. **Foundation** — Room (`data/`) persists every message; DataStore
   (`settings/`) holds the device id/name, server-mode toggle, and server IP;
   the Compose UI (`ui/`) is a single chat screen + a settings screen.
2. **P2P (same-network TCP star)** — `net/`: the hub (`MeshServer`) listens;
   every other device (`MeshClient`) connects to it; the hub relays each frame
   to all other peers. A foreground `LiqMeshService` keeps the socket alive in
   the background. **Wi-Fi Direct full mesh was deliberately NOT adopted** for
   the hackathon (auto-formed mesh / NAT traversal is out of scope — it is a
   Vision item; this build requires one shared Wi-Fi or hotspot LAN).
3. **Offline sync** — `MeshController`: a send that misses the socket stays
   queued (outbox) and is replayed in order on reconnect; a freshly-joined
   device backfills history it missed from the hub. DAO `OnConflict.IGNORE`
   makes every path idempotent (no dup messages).
4. **Server-only AI** — only the hub holds the model and the authoritative
   history, so only the hub summarises. Generation runs off the relay path on
   its own single-thread dispatcher with single-flight de-dup, so plain chat
   keeps flowing while a summary is being generated ("chat first").

## Build / run

### (A) Android Studio

1. **Open** → select the `android/` folder.
2. Create the git-ignored `local.properties`:
   `sdk.dir=/Users/<you>/Library/Android/sdk`
3. Let Gradle sync (LEAP artifacts come from Maven Central; no extra repo).
4. Run on a **physical arm64 Android device** (the supported target).

### (B) Command line

The system JDK may be too new for AGP; use the JBR bundled with Android Studio.

```bash
cd android
export ANDROID_HOME="$HOME/Library/Android/sdk"
./gradlew :app:assembleDebug \
  -Dorg.gradle.java.home="/Applications/Android Studio.app/Contents/jbr/Contents/Home"
# APK: app/build/outputs/apk/debug/app-debug.apk
```

Headless emulator smoke (the AVD used here is `leap_smoke`):

```bash
"$ANDROID_HOME/emulator/emulator" -avd leap_smoke \
  -no-window -no-audio -gpu swiftshader_indirect &
adb wait-for-device
adb install -r app/build/outputs/apk/debug/app-debug.apk
adb shell am start -n ai.liquidway.lfmsmoke/.MainActivity
```

Run the deterministic JVM tests (relay + offline sync + layer-4 summary
policy, no emulator needed):

```bash
./gradlew :app:testDebugUnitTest \
  -Dorg.gradle.java.home="/Applications/Android Studio.app/Contents/jbr/Contents/Home"
```

## Hackathon demo (two physical phones, one shared Wi-Fi / hotspot)

Put both phones on the **same Wi-Fi network or the same phone's hotspot**.

1. **Phone A — become the hub.** Open Settings, turn **Server mode ON**.
   The chat header shows `Hub · waiting for peers`.
2. **Find A's IP.** On phone A: Settings (Android) → Wi-Fi → the connected
   network → note the IPv4 address (e.g. `192.168.x.y`).
3. **Phone B — join as a leaf.** Open Settings, leave **Server mode OFF**, and
   enter **phone A's IP** in the server-host field. The header on both phones
   flips to `Connected` / `Hub · 1 peer(s) connected`.
4. **Chat both ways.** Send messages from A and from B; each appears on the
   other within a moment (relayed through the hub).
5. **Prove offline sync.** Put **phone B in airplane mode**, then send 1–2
   messages on B — the header shows `… queued` (they are held LOCAL). Turn
   airplane mode **off**; on reconnect B flushes the queue in order and also
   backfills anything it missed. Nothing is lost or duplicated.
6. **AI situational summary.** On either phone tap the **info icon ("状況
   まとめ")** in the top bar. The request reaches **hub A**, A's on-device LFM
   summarises the recent conversation, and the summary is relayed back — it
   appears as a distinct centred card on **both** phones. (First ever summary
   triggers the one-time model download on A; see constraints.)

If a summary cannot run, the app says so in a snackbar — "サーバー未接続"
when a leaf has no link to the hub, or a model-load / inference error pushed
from the hub. No silent failures.

## Known constraints (honest)

- **Two physical phones are the real validation path.** Two emulators cannot
  reach each other's TCP sockets (each sits behind its own user-mode SLIRP
  NAT), so cross-device chat must be demoed on real hardware on one LAN. The
  JVM tests prove the protocol over real loopback sockets; the headless
  emulator only proves the app launches and the LEAP model loads/generates.
- **Model:** `LFM2-350M` @ `Q8_0`. The **first** summary on the hub downloads
  the model (network required once); it is then cached on device. No API key
  or account — on-device inference only.
- **minSdk 31, `arm64-v8a` only**, 3GB+ RAM recommended (LEAP runtime is
  arm64 native libs; documented to possibly crash loading bundles on
  emulators — physical device is the supported target).
- **No automatic Wi-Fi Direct mesh.** Devices must share one Wi-Fi/hotspot and
  the leaf must be told the hub's IP. Auto-discovery / full mesh is a Vision
  item, not in this build.

## Notes on scope

This MVP was scoped to roughly **9 hours of real build time** and was kept
deliberately tight: the four layers above plus low-risk polish (visible error
messages, idle model release, per-request coroutine scoping). No speculative
features were added beyond what a two-phone disaster-chat demo needs.

### Polish in this build

- **Visible errors:** summary failures surface in a snackbar (leaf "not
  connected" and hub-side model/inference errors), instead of failing
  silently in the log.
- **Idle model release:** when a hub leaves server mode (or the service
  stops), the controller calls the engine's `release()`, which invokes the
  LEAP SDK's `ModelRunner.unload()` to return native memory; the next summary
  lazily reloads.
- **Per-request scope:** the in-flight summary coroutine is tracked and
  cancelled on any transport reconfigure (mode/host switch, service stop) so
  a stale generation can never relay onto a torn-down socket or leak.
