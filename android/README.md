# LFM Smoke (LEAP SDK on-device)

Minimal Kotlin + Jetpack Compose app that runs a Liquid Foundation Model
(LFM) **on-device** via the [LEAP SDK](https://leap.liquid.ai/), as the
technical de-risking step for LiqMesh (disaster-time P2P chat).

It loads the smallest LFM text model, runs one prompt, and reports the
generated token count, elapsed time, and tokens/second.

## What was verified against official docs

Sources: <https://leap.liquid.ai/>, <https://docs.liquid.ai/>,
the LEAP Android quick start, the `ai.liquid.leap` Maven Central artifacts,
and the official [Liquid4All/LeapSDK-Examples](https://github.com/Liquid4All/LeapSDK-Examples)
repo (used to ground the exact API surface).

| Topic | Finding |
|---|---|
| **Emulator support** | LEAP docs state verbatim: *"The SDK may crash on loading model bundles in emulators. Always test on a physical device."* Officially **best-effort / unsupported**; a physical device is the supported target. In practice it **did work** on a headless arm64-v8a emulator here (see Measurement results) — but treat that as not guaranteed. |
| Gradle dependency | `ai.liquid.leap:leap-sdk:0.10.6` + `ai.liquid.leap:leap-model-downloader:0.10.6` (Maven Central, no extra repo config) |
| Build toolchain | Kotlin 2.3+, Android Gradle Plugin 8.13+, JDK 17. This project pins AGP 8.13.2 / Kotlin 2.3.21 / Gradle 8.14. `compileSdk = 36` (forced by transitive `androidx.core 1.17.0`). |
| minSdk / ABI | minSdk 31 (Android 12), **`arm64-v8a` only**, 3GB+ RAM recommended |
| Model | `LFM2-350M` @ `Q8_0` — smallest LFM text model in the LEAP library; downloaded at runtime on first launch (needs network), then cached on device |
| Generation API | `ModelRunner.createConversation().generateResponse(prompt): Flow<MessageResponse>`; collect `Chunk` for text, `Complete` for `GenerationStats` (`promptTokens`, `completionTokens`, `tokenPerSecond`) |
| Auth / API key | **None.** On-device inference needs no API key or account; model pulls from the public LEAP model library |

## Project layout

```
android/
├── settings.gradle.kts
├── build.gradle.kts            # AGP 8.13.2 / Kotlin 2.3.21
├── gradle.properties
├── gradle/wrapper/             # Gradle 8.14 wrapper + jar
├── gradlew
└── app/
    ├── build.gradle.kts        # LEAP deps, minSdk 31, arm64-v8a
    ├── proguard-rules.pro
    └── src/main/
        ├── AndroidManifest.xml
        ├── res/values/strings.xml
        └── java/ai/liquidway/lfmsmoke/
            ├── LfmEngine.kt        # LEAP wrapper: download → load → generate + metrics
            ├── SmokeViewModel.kt   # orchestration + logcat metrics emission
            ├── SmokeScreen.kt      # Compose UI: prompt / run / response / metrics
            └── MainActivity.kt     # entrypoint + debug smoke intent extra
```

The only hand-written, review-worthy LEAP integration is `LfmEngine.kt`,
the LEAP blocks in `app/build.gradle.kts`, and the UI/VM glue. Everything
else is standard Android scaffolding boilerplate.

## Open in Android Studio

1. Open Android Studio → **Open** → select the `android/` folder.
2. `local.properties` is git-ignored; create it with your SDK path:
   `sdk.dir=/Users/<you>/Library/Android/sdk`
3. Let Gradle sync (fetches LEAP artifacts from Maven Central).
4. **Run on a physical arm64 Android device** (developer mode + USB debugging
   enabled). First launch downloads `LFM2-350M` (network required), then runs.

## Build / run from the CLI

The system JDK may be too new for AGP; use the JBR bundled with Android Studio:

```bash
cd android
export ANDROID_HOME="$HOME/Library/Android/sdk"
./gradlew -Dorg.gradle.java.home="/Applications/Android Studio.app/Contents/jbr/Contents/Home" \
  :app:assembleDebug
```

Output APK: `app/build/outputs/apk/debug/app-debug.apk`

### Run on a connected physical device (supported path)

```bash
adb install -r app/build/outputs/apk/debug/app-debug.apk
# Headless smoke: launch with a prompt extra, results go to logcat
adb shell am start -n ai.liquidway.lfmsmoke/.MainActivity \
  -e smoke_prompt "Say hello to disaster responders in one short sentence."
adb logcat -s LfmSmoke:I
```

Look for these logcat lines (tag `LfmSmoke`):

```
SMOKE_RESULT  text=<generated text>
SMOKE_METRICS generated_tokens=… prompt_tokens=… elapsed_s=… sdk_tok_s=… wall_tok_s=…
```

`sdk_tok_s` is reported by the LEAP SDK's `GenerationStats`; `wall_tok_s`
is an independent wall-clock cross-check computed in `LfmEngine`.

### Emulator note

The LEAP model runtime (llama.cpp / ggml backend, shipped as `arm64-v8a`
native libs) is documented to potentially crash when loading model bundles
on emulators. The emulator path is attempted for completeness but is **not**
the supported target — see the measurement section below for the actual
observed outcome on this machine.

## Measurement results

Despite the official "may crash on emulator" warning, the smoke run
**succeeded on a headless arm64-v8a emulator** on Apple Silicon
(`system-images;android-35;google_apis;arm64-v8a`, ~2GB RAM AVD,
`-gpu swiftshader_indirect`). No crash.

Real measured values (logcat tag `LfmSmoke`, model `LFM2-350M/Q8_0`,
2026-05-17), **not fabricated**:

```
SMOKE_RESULT  text=I see you've typed "say," but how can I assist you today?
              If you need help with a specific topic, feel free to ask!
SMOKE_METRICS generated_tokens=31 prompt_tokens=11 elapsed_s=2.296
              sdk_tok_s=13.74 wall_tok_s=13.50
```

LEAP engine's own breakdown (tag `LiquidInferenceEngine`) corroborates:

| Metric | Value |
|---|---|
| Model load time | 4.22 s |
| Total inference time | 2.26 s @ 13.74 tok/s |
| Prompt evaluation (11 tok) | 1.60 s @ 6.88 tok/s |
| Generation (31 tok) | 0.66 s @ 47.19 tok/s |
| Time to first token | 1.60 s |

`sdk_tok_s` (13.74) and the independent `wall_tok_s` (13.50) agree
closely, and both match the SDK's own reported rate.

> Caveat: these are **emulator (swiftshader CPU)** numbers on a 2GB AVD.
> On-device performance on real arm64 hardware will differ (typically
> faster). The point of this run was to prove LEAP loads and generates
> on-device end to end — which it does.

To reproduce, see "Run on a connected physical device" above, or the
emulator steps in the PR description.
