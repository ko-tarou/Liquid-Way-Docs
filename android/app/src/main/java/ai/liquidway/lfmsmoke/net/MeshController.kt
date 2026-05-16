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
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * The single owner of the transport, the single send window, and the layer-3
 * offline-sync policy.
 *
 * Layer 3 responsibilities (all funnelled here so the UI/transport stay
 * untouched):
 *  - **Outbox**: a send that misses the socket leaves the row LOCAL. On every
 *    (re)connect [flushOutbox] replays LOCAL rows in createdAt order; success
 *    flips them SENT. A [Mutex] makes the flush single-flight so a reconnect
 *    storm cannot double-send.
 *  - **Backfill**: a freshly-connected leaf asks the hub for everything created
 *    after its high-water mark; the hub answers from its store (capped). Both
 *    directions insert through the repository, so the DAO's OnConflict.IGNORE
 *    guarantees idempotency.
 *
 * Process-wide singleton: the foreground service and the ViewModels share one
 * instance so connection state is consistent everywhere.
 */
class MeshController private constructor(
    private val repository: MessageRepository,
    private val settings: SettingsRepository,
) : MeshEvents {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Volatile
    private var transport: MeshTransport? = null

    // Cancelled on every reconfigure so a stale transport's StateFlow can't
    // keep feeding the controller's stream.
    private var stateMirrorJob: Job? = null

    // Single-flight guard for the outbox drain. Prevents a reconnect storm (or
    // hub + leaf both firing onLinkEstablished) from sending a message twice.
    private val flushMutex = Mutex()

    private val _state = MutableStateFlow<MeshState>(MeshState.Idle)
    val state: StateFlow<MeshState> = _state.asStateFlow()

    /** Live count of un-sent (LOCAL) messages, for the subtle UI badge. */
    val pendingCount: StateFlow<Int> by lazy {
        val flow = MutableStateFlow(0)
        scope.launch {
            repository.pendingCount.collect { flow.value = it }
        }
        flow.asStateFlow()
    }

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
            MeshServer(events = this)
        } else {
            if (host.isBlank()) {
                Log.w(TAG, "Client mode but no server host set; staying idle")
                _state.value = MeshState.Disconnected("no server IP set")
                return
            }
            MeshClient(host = host, events = this)
        }
        transport = t
        // Mirror the transport's state into the controller's stream.
        stateMirrorJob = scope.launch { t.state.collect { _state.value = it } }
        t.start()
        Log.i(TAG, "Configured serverMode=$serverMode host='$host'")
    }

    // ---- MeshEvents ------------------------------------------------------

    /** Persist an inbound peer message. Returns true if it was new. */
    override suspend fun onMessage(message: Message): Boolean =
        repository.acceptRemote(message)

    /**
     * Answer a peer's backfill request: replay our messages created after
     * [since], capped by age and count, oldest-first as sync_resp frames.
     */
    override suspend fun onSyncRequest(since: Long, reply: suspend (line: String) -> Unit) {
        val floor = maxOf(since, System.currentTimeMillis() - BACKFILL_MAX_AGE_MS)
        val missing = repository.backfillSince(floor, BACKFILL_MAX_MESSAGES)
        if (missing.isEmpty()) return
        Log.i(TAG, "Backfilling ${missing.size} message(s) since $since")
        for (m in missing) reply(MessageWire.encodeSyncResp(m))
    }

    /**
     * A link came up. On the leaf: ask for backfill, then flush the outbox.
     * The hub answers backfill reactively (see [onSyncRequest]) and has nothing
     * to push proactively, so for it this is a no-op beyond a log line.
     */
    override suspend fun onLinkEstablished(transport: MeshTransport) {
        if (transport is MeshClient) {
            val since = repository.latestCreatedAt()
            transport.sendRaw(MessageWire.encodeSyncReq(since))
        }
        flushOutbox()
    }

    // ---- Outbox ----------------------------------------------------------

    /**
     * Drain LOCAL messages onto the live socket in createdAt order. Single
     * -flight via [flushMutex]; stops at the first failure so order is
     * preserved and the rest stay queued for the next link.
     */
    private suspend fun flushOutbox() {
        if (!flushMutex.tryLock()) {
            Log.d(TAG, "Outbox flush already running; skipping")
            return
        }
        try {
            val t = transport ?: return
            val pending = repository.outbox()
            if (pending.isEmpty()) return
            Log.i(TAG, "Flushing ${pending.size} queued message(s)")
            for (m in pending) {
                if (t.send(m)) {
                    repository.markSent(m.id)
                } else {
                    Log.d(TAG, "Outbox flush stalled at ${m.id} (link down)")
                    break
                }
            }
        } finally {
            flushMutex.unlock()
        }
    }

    /**
     * Local-send hook (called by the repository). Flips LOCAL -> SENT only on
     * a real socket handoff; a dead link leaves the row LOCAL for the outbox.
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
            Log.d(TAG, "Send returned false; ${message.id} stays LOCAL (outbox)")
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

        /**
         * Backfill caps. A peer is sent at most [BACKFILL_MAX_MESSAGES] of the
         * most recent messages, and never anything older than
         * [BACKFILL_MAX_AGE_MS]. Bounds the catch-up burst so a long-lived hub
         * does not replay an unbounded history to every new joiner.
         */
        private const val BACKFILL_MAX_MESSAGES = 500
        private const val BACKFILL_MAX_AGE_MS = 24L * 60 * 60 * 1000 // 24h

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
