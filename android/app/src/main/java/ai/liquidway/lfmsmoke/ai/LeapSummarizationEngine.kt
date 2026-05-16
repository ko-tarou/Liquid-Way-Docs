package ai.liquidway.lfmsmoke.ai

import ai.liquidway.lfmsmoke.LfmEngine
import ai.liquidway.lfmsmoke.data.Message
import android.content.Context
import android.util.Log
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Production [SummarizationEngine] backed by the already-proven [LfmEngine]
 * (LEAP SDK, LFM2-350M on-device).
 *
 * This class deliberately does NOT modify [LfmEngine]: it composes it. The
 * LEAP download/load/generate path that was smoke-validated on the emulator is
 * reused verbatim — `loadModel()` then `generate(prompt)` — so layer 4 cannot
 * regress the layer-0 LEAP proof.
 *
 * Concurrency: a single [Mutex] serialises model load + generation. The LEAP
 * ModelRunner is a single heavyweight resource; serialising avoids two summary
 * requests trying to drive one runner concurrently. The caller already runs
 * this off the chat relay path, so blocking here never stalls plain chat.
 *
 * @param onLoadProgress optional hook so the UI can show first-run model
 *   download progress (0..1). Generation itself reports no fine-grained
 *   progress; the UI shows an indeterminate "summarising" state.
 */
class LeapSummarizationEngine(
    context: Context,
    private val engine: LfmEngine = LfmEngine(context.applicationContext),
    private val onLoadProgress: (Float) -> Unit = {},
) : SummarizationEngine {

    private companion object {
        const val TAG = "LiqMesh/AI"
    }

    // Serialises load + generate: one LEAP runner, one summary at a time.
    private val gate = Mutex()

    @Volatile
    private var loaded = false

    override suspend fun summarize(messages: List<Message>): String {
        val prompt = SummarizationEngine.buildPrompt(messages)
        gate.withLock {
            if (!loaded) {
                Log.i(TAG, "Lazy-loading LFM model for summarisation")
                engine.loadModel(onLoadProgress)
                loaded = true
                Log.i(TAG, "LFM model ready")
            }
            Log.i(TAG, "Generating summary over ${messages.size} message(s)")
            val result = engine.generate(prompt)
            val text = result.text.trim()
            Log.i(
                TAG,
                "Summary generated in ${"%.1f".format(result.elapsedSeconds)}s " +
                    "(${result.sdkGeneratedTokens ?: -1} tok)",
            )
            return text.ifBlank {
                "（要約を生成できませんでした）"
            }
        }
    }

    /**
     * Unloads the underlying LFM under the same [gate] that serialises
     * load/generate, so a release can never race a summary in flight: it
     * waits for the current generation to finish, then frees the model and
     * clears [loaded] so the next [summarize] lazily reloads. Best-effort
     * (see [LfmEngine.unload]); never throws.
     */
    override suspend fun release() {
        gate.withLock {
            if (!loaded) return
            Log.i(TAG, "Releasing idle LFM model")
            engine.unload()
            loaded = false
        }
    }
}
