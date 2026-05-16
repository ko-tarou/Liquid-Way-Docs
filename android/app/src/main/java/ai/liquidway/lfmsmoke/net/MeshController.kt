package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageRepository
import ai.liquidway.lfmsmoke.data.MessageStatus
import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.content.Context
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/**
 * The single owner of the layer-2 transport and the single send window.
 *
 * Why this exists: the inbound-persist policy and the local-send path must live
 * in exactly one place so layer 3 (offline outbox / resend) can wrap [send]
 * without touching the UI or the transport. [MessageRepository.transportSender]
 * is pointed here, so `repository.addLocal(...)` already flows through the
 * mesh — the UI keeps calling the repository and nothing else.
 *
 * Process-wide singleton: the foreground service and the ViewModels share one
 * instance so connection state is consistent everywhere.
 */
class MeshController private constructor(
    private val repository: MessageRepository,
    private val settings: SettingsRepository,
) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile
    private var transport: MeshTransport? = null

    // Cancelled on every reconfigure so a stale transport's StateFlow can't
    // keep feeding the controller's stream.
    private var stateMirrorJob: Job? = null

    private val _state = MutableStateFlow<MeshState>(MeshState.Idle)
    val state: StateFlow<MeshState> = _state.asStateFlow()

    init {
        // Route every locally-authored message through the live transport.
        repository.transportSender = { msg -> sendLocal(msg) }
    }

    /**
     * (Re)configures the transport for the given mode. Safe to call on every
     * settings change: it tears down the previous transport first.
     */
    suspend fun configure(serverMode: Boolean, host: String) {
        teardown()
        val t: MeshTransport = if (serverMode) {
            MeshServer(onInbound = ::persistInbound)
        } else {
            if (host.isBlank()) {
                Log.w(TAG, "Client mode but no server host set; staying idle")
                _state.value = MeshState.Disconnected("no server IP set")
                return
            }
            MeshClient(host = host, onInbound = ::persistInbound)
        }
        transport = t
        // Mirror the transport's state into the controller's stream.
        stateMirrorJob = scope.launch { t.state.collect { _state.value = it } }
        t.start()
        Log.i(TAG, "Configured serverMode=$serverMode host='$host'")
    }

    /** Persist an inbound peer message. Returns true if it was new. */
    private suspend fun persistInbound(message: Message): Boolean =
        repository.acceptRemote(message)

    /**
     * Local-send hook (called by the repository). Flips LOCAL -> SENT only on
     * a real socket handoff; a dead link leaves the row LOCAL for layer 3.
     */
    private suspend fun sendLocal(message: Message) {
        val t = transport
        if (t == null) {
            Log.d(TAG, "No transport; ${message.id} stays LOCAL")
            return
        }
        if (t.send(message)) {
            repository.markSent(message.id)
        } else {
            Log.d(TAG, "Send returned false; ${message.id} stays LOCAL")
        }
    }

    private suspend fun teardown() {
        stateMirrorJob?.cancel()
        stateMirrorJob = null
        transport?.stop()
        transport = null
        _state.value = MeshState.Idle
    }

    /** Full stop (service destroyed). */
    suspend fun shutdown() = teardown()

    companion object {
        private const val TAG = "LiqMesh/Ctl"

        @Volatile
        private var instance: MeshController? = null

        fun get(context: Context): MeshController {
            return instance ?: synchronized(this) {
                instance ?: MeshController(
                    repository = MessageRepository.get(context),
                    settings = SettingsRepository.get(context),
                ).also { instance = it }
            }
        }
    }
}

/** Status flip helper kept next to the controller for cohesion. */
internal suspend fun MessageRepository.markSent(id: String) =
    updateStatus(id, MessageStatus.SENT)
