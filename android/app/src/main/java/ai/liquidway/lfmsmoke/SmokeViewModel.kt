package ai.liquidway.lfmsmoke

import android.app.Application
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/** UI state for the smoke screen. */
data class SmokeUiState(
    val status: String = "Idle",
    val busy: Boolean = false,
    val responseText: String = "",
    val metricsLine: String = "",
)

class SmokeViewModel(app: Application) : AndroidViewModel(app) {

    private val engine = LfmEngine(app.applicationContext)

    private val _state = MutableStateFlow(SmokeUiState())
    val state: StateFlow<SmokeUiState> = _state.asStateFlow()

    /** Runs load + generate for [prompt], updating UI state and emitting logcat metrics. */
    fun runSmoke(prompt: String) {
        if (_state.value.busy) return
        viewModelScope.launch {
            _state.value = SmokeUiState(status = "Loading model...", busy = true)
            try {
                engine.loadModel { p ->
                    _state.value = _state.value.copy(
                        status = "Downloading model: ${(p * 100).toInt()}%"
                    )
                }
                _state.value = _state.value.copy(status = "Generating...")

                val r = engine.generate(prompt)

                val metrics = buildString {
                    append("generated_tokens=${r.sdkGeneratedTokens ?: "n/a"} ")
                    append("prompt_tokens=${r.sdkPromptTokens ?: "n/a"} ")
                    append("elapsed_s=%.3f ".format(r.elapsedSeconds))
                    append("sdk_tok_s=${r.sdkTokenPerSecond?.let { "%.2f".format(it) } ?: "n/a"} ")
                    append(
                        "wall_tok_s=${
                            r.wallClockTokensPerSecond?.let { "%.2f".format(it) } ?: "n/a"
                        }"
                    )
                }

                // Machine-parseable smoke result for `adb logcat`.
                Log.i(LfmEngine.TAG, "SMOKE_RESULT text=${r.text.replace("\n", "\\n")}")
                Log.i(LfmEngine.TAG, "SMOKE_METRICS $metrics")

                _state.value = SmokeUiState(
                    status = "Done",
                    busy = false,
                    responseText = r.text,
                    metricsLine = metrics,
                )
            } catch (e: Throwable) {
                Log.e(LfmEngine.TAG, "SMOKE_ERROR ${e.message}", e)
                _state.value = SmokeUiState(
                    status = "Error: ${e.message}",
                    busy = false,
                )
            }
        }
    }
}
