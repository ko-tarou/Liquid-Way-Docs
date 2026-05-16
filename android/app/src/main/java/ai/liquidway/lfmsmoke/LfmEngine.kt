package ai.liquidway.lfmsmoke

import ai.liquid.leap.ModelRunner
import ai.liquid.leap.downloader.LeapModelDownloader
import ai.liquid.leap.downloader.LeapModelDownloaderNotificationConfig
import ai.liquid.leap.message.MessageResponse
import android.content.Context
import android.util.Log
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.takeWhile

/**
 * Minimal wrapper around the LEAP SDK for on-device LFM inference.
 *
 * Responsibilities:
 *  - lazily download + load the smallest practical LFM text model,
 *  - run a single prompt and return the generated text plus metrics.
 *
 * The SDK is documented to potentially crash when loading model bundles on
 * emulators; a physical arm64-v8a device is the supported target.
 */
class LfmEngine(private val context: Context) {

    companion object {
        const val TAG = "LfmSmoke"

        // Smallest LFM text model in the LEAP model library. Q8_0 keeps quality
        // high while staying well under the 3GB RAM guidance for smoke testing.
        private const val MODEL_NAME = "LFM2-350M"
        private const val QUANTIZATION_SLUG = "Q8_0"
    }

    /** Result of a single generation, including both SDK and wall-clock metrics. */
    data class Result(
        val text: String,
        // Metrics reported by the LEAP SDK (GenerationStats), null if absent.
        val sdkGeneratedTokens: Long?,
        val sdkPromptTokens: Long?,
        val sdkTokenPerSecond: Float?,
        // Independent wall-clock measurement (does not rely on SDK internals).
        val elapsedSeconds: Double,
        val wallClockTokensPerSecond: Double?,
    )

    private var downloader: LeapModelDownloader? = null
    private var modelRunner: ModelRunner? = null

    private fun ensureDownloader(): LeapModelDownloader {
        return downloader ?: LeapModelDownloader(
            context,
            notificationConfig = LeapModelDownloaderNotificationConfig.build {
                notificationTitleDownloading = "Downloading LFM model"
                notificationTitleDownloaded = "LFM model ready"
            }
        ).also { downloader = it }
    }

    /**
     * Downloads (first run only) and loads the model.
     *
     * @param onProgress invoked with a 0..1 progress fraction during download.
     */
    suspend fun loadModel(onProgress: (Float) -> Unit = {}) {
        if (modelRunner != null) return
        val dl = ensureDownloader()

        val status = dl.queryStatus(MODEL_NAME, QUANTIZATION_SLUG)
        if (status is LeapModelDownloader.ModelDownloadStatus.NotOnLocal) {
            Log.i(TAG, "Model not present locally; starting download")
            val progressFlow = dl.observeDownloadProgress(MODEL_NAME, QUANTIZATION_SLUG)
            dl.requestDownloadModel(MODEL_NAME, QUANTIZATION_SLUG)
            progressFlow
                .onEach { p ->
                    if (p != null && p.totalSizeInBytes > 0) {
                        onProgress(p.downloadedSizeInBytes.toFloat() / p.totalSizeInBytes)
                    }
                }
                .takeWhile { p ->
                    p != null ||
                        dl.queryStatus(MODEL_NAME, QUANTIZATION_SLUG) is
                        LeapModelDownloader.ModelDownloadStatus.DownloadInProgress
                }
                .collect()
        }

        Log.i(TAG, "Loading model $MODEL_NAME/$QUANTIZATION_SLUG")
        modelRunner = dl.loadModel(MODEL_NAME, QUANTIZATION_SLUG)
        Log.i(TAG, "Model loaded")
    }

    /**
     * Runs a single prompt to completion and returns the text + metrics.
     * [loadModel] must have completed first.
     */
    suspend fun generate(prompt: String): Result {
        val runner = modelRunner
            ?: error("Model not loaded. Call loadModel() first.")

        val conversation = runner.createConversation()
        val sb = StringBuilder()

        var sdkGenTokens: Long? = null
        var sdkPromptTokens: Long? = null
        var sdkTps: Float? = null

        val startNs = System.nanoTime()
        var caught: Throwable? = null

        conversation.generateResponse(prompt)
            .catch { e -> caught = e }
            .collect { response ->
                when (response) {
                    is MessageResponse.Chunk -> sb.append(response.text)
                    is MessageResponse.Complete -> {
                        val stats = response.stats
                        if (stats != null) {
                            sdkGenTokens = stats.completionTokens
                            sdkPromptTokens = stats.promptTokens
                            sdkTps = stats.tokenPerSecond
                        }
                    }
                    else -> Unit
                }
            }

        val elapsedSeconds = (System.nanoTime() - startNs) / 1_000_000_000.0

        caught?.let { throw it }

        val genTokens = sdkGenTokens
        val wallTps = if (genTokens != null && elapsedSeconds > 0) {
            genTokens / elapsedSeconds
        } else {
            null
        }

        return Result(
            text = sb.toString(),
            sdkGeneratedTokens = sdkGenTokens,
            sdkPromptTokens = sdkPromptTokens,
            sdkTokenPerSecond = sdkTps,
            elapsedSeconds = elapsedSeconds,
            wallClockTokensPerSecond = wallTps,
        )
    }

    /**
     * Releases the loaded model and frees its native memory.
     *
     * Uses the LEAP SDK's own `ModelRunner.unload()` (verified present in
     * leap-sdk 0.10.6). After this the next [generate] requires [loadModel]
     * again. A no-op if nothing is loaded, and best-effort: an unload failure
     * is logged but never propagated, since freeing memory must not crash the
     * hub. The downloader handle is intentionally kept (a cheap object) so a
     * re-load does not re-query the model library from scratch.
     */
    suspend fun unload() {
        val runner = modelRunner ?: return
        try {
            runner.unload()
            Log.i(TAG, "Model unloaded; native memory released")
        } catch (e: Exception) {
            Log.w(TAG, "Model unload failed (ignored): ${e.message}")
        } finally {
            modelRunner = null
        }
    }
}
