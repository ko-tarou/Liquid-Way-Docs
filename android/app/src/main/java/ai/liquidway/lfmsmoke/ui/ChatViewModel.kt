package ai.liquidway.lfmsmoke.ui

import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageRepository
import ai.liquidway.lfmsmoke.net.MeshController
import ai.liquidway.lfmsmoke.net.MeshState
import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

class ChatViewModel(app: Application) : AndroidViewModel(app) {

    private val repository = MessageRepository.get(app)
    private val settings = SettingsRepository.get(app)
    private val mesh = MeshController.get(app)

    /** Live layer-2 connection state for the chat header. */
    val meshState: StateFlow<MeshState> = mesh.state
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = MeshState.Idle,
        )

    val messages: StateFlow<List<Message>> = repository.messages
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = emptyList(),
        )

    /** Number of locally-authored messages still queued (outbox), for the UI. */
    val pendingCount: StateFlow<Int> = repository.pendingCount
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = 0,
        )

    /** This device's stable id, used by the UI to align bubbles (self/other). */
    val deviceId: StateFlow<String> = kotlinx.coroutines.flow.flow {
        emit(settings.deviceId())
    }.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5_000),
        initialValue = "",
    )

    /**
     * True while the hub is generating a summary. On a leaf this reflects the
     * hub's state only after the AI message arrives; the local "requested"
     * latch ([summaryRequested]) covers the leaf's own button feedback.
     */
    val summaryRunning: StateFlow<Boolean> = mesh.summaryRunning
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5_000),
            initialValue = false,
        )

    private val _summaryRequested = MutableStateFlow(false)

    /** Local latch so the button shows progress even on a leaf (no hub state). */
    val summaryRequested: StateFlow<Boolean> = _summaryRequested.asStateFlow()

    private val _summaryError = MutableStateFlow<String?>(null)

    /**
     * User-facing summary problems, from two sources merged onto one snackbar:
     *  - leaf-side "not connected" set locally in [requestSummary], and
     *  - hub-side generation failures pushed by [MeshController.summaryError]
     *    (model load / inference error).
     * The first non-null of either wins; [dismissSummaryError] clears both.
     */
    val summaryError: StateFlow<String?> = _summaryError.asStateFlow()

    init {
        // Mirror hub-side generation failures into the local error channel so
        // the existing snackbar shows them too (no new UI surface added).
        viewModelScope.launch {
            mesh.summaryError.collect { hubError ->
                if (hubError != null && _summaryError.value == null) {
                    _summaryError.value = hubError
                }
                if (hubError != null) {
                    _summaryRequested.value = false
                }
            }
        }
    }

    fun send(body: String) {
        viewModelScope.launch { repository.addLocal(body) }
    }

    /**
     * Triggers the "状況まとめ" flow. The latch clears either when the hub
     * reports it finished or, on a leaf, after the relayed AI message lands
     * (messages flow change) — see [clearSummaryLatchOnResult].
     */
    fun requestSummary() {
        if (_summaryRequested.value) return
        _summaryRequested.value = true
        _summaryError.value = null
        viewModelScope.launch {
            val ok = mesh.requestSummary()
            if (!ok) {
                _summaryRequested.value = false
                _summaryError.value = "サーバー未接続：まとめを実行できません"
            }
        }
    }

    fun dismissSummaryError() {
        _summaryError.value = null
        // Also clear the hub-side latch so it does not immediately re-emit
        // the same error back into the merged channel above.
        mesh.clearSummaryError()
    }

    /** Clears the local latch once a fresh AI summary message has arrived. */
    fun clearSummaryLatchOnResult(latestIsAi: Boolean) {
        if (latestIsAi && _summaryRequested.value && !summaryRunning.value) {
            _summaryRequested.value = false
        }
    }
}
