package ai.liquidway.lfmsmoke.ui

import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageRepository
import ai.liquidway.lfmsmoke.net.MeshController
import ai.liquidway.lfmsmoke.net.MeshState
import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.app.Application
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
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

    fun send(body: String) {
        viewModelScope.launch { repository.addLocal(body) }
    }
}
