package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.ai.SummarizationEngine
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageRepository
import ai.liquidway.lfmsmoke.data.MessageStatus
import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.content.Context
import android.util.Log
import kotlinx.coroutines.CancellationException
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
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

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

    /**
     * Layer-4 "chat-priority" dispatcher. Summary generation is heavyweight
     * (LFM inference); running it on its own single-thread, limited-parallelism
     * dispatcher keeps it OFF the IO pool that carries socket reads/writes, so
     * a generation in flight never starves plain-chat relay. This is the
     * minimal "chat first" mechanism — no elaborate QoS, just isolation +
     * single-flight (see [summaryInFlight]).
     */
    private val summaryDispatcher =
        Dispatchers.IO.limitedParallelism(1)

    /** Single-flight guard: ignore a second summary_req while one is running. */
    private val summaryInFlight = AtomicBoolean(false)

    /**
     * Injected by the host (the service) before any summary_req can arrive.
     * Null on a pure leaf or before injection — a summary_req is then ignored.
     * Kept as a lambda so the heavyweight LEAP engine is constructed lazily and
     * tests can supply a deterministic fake.
     */
    @Volatile
    var summarizationEngineProvider: (() -> SummarizationEngine)? = null

    @Volatile
    private var resolvedEngine: SummarizationEngine? = null

    /** Whether THIS device is the hub (only the hub runs the LFM). */
    @Volatile
    private var serverMode: Boolean = false

    private val _summaryRunning = MutableStateFlow(false)

    /** True while the hub is generating a summary (drives the UI spinner). */
    val summaryRunning: StateFlow<Boolean> = _summaryRunning.asStateFlow()

    private val _summaryError = MutableStateFlow<String?>(null)

    /**
     * Set to a user-facing string when a hub-side generation fails (model
     * load/inference error). The UI surfaces it through the same snackbar as
     * the leaf-side "not connected" message and then calls [clearSummaryError].
     * Hub-only signal; a leaf only ever sees its own local "not connected".
     */
    val summaryError: StateFlow<String?> = _summaryError.asStateFlow()

    /** Clears the hub-side error latch once the UI has shown it. */
    fun clearSummaryError() {
        _summaryError.value = null
    }

    // The in-flight summary coroutine. Tracked so a transport reconfigure
    // (mode/host switch, service stop) deterministically cancels it instead
    // of letting it run on against a torn-down transport (per-request scope).
    @Volatile
    private var summaryJob: Job? = null

    @Volatile
    private var transport: MeshTransport? = null

    /**
     * Stage-1 bridge: the secondary outbound link to a *second* hub, present
     * only when this device is a hub AND bridging is enabled+configured. Null in
     * every other case — and null is the whole zero-regression story: when the
     * bridge is absent, [ingest]/[onMessage]/[sendLocal] take exactly the
     * pre-bridge code paths, so the star behaves byte-for-byte as before.
     */
    @Volatile
    private var bridge: MeshClient? = null

    // Cancelled on every reconfigure so a stale transport's StateFlow can't
    // keep feeding the controller's stream.
    private var stateMirrorJob: Job? = null

    // Mirrors the bridge MeshClient's own connect/disconnect into the Hub state.
    private var bridgeStateJob: Job? = null

    // Latest raw state of each link, so [foldBridge] can recompute the published
    // Hub(bridge=…) from whichever stream just changed without losing the other.
    @Volatile
    private var lastPrimaryState: MeshState = MeshState.Idle

    @Volatile
    private var lastBridgeState: MeshState = MeshState.Idle

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
     * settings change: it tears down the previous transport (and bridge) first.
     *
     * The Stage-1 bridge is created ONLY when this device is a hub AND bridging
     * is both enabled and pointed at a host. In every other combination [bridge]
     * stays null — including the default (bridgeEnabled=false) — so the
     * pre-bridge star behaviour is reproduced exactly (zero regression).
     */
    suspend fun configure(
        serverMode: Boolean,
        host: String,
        bridgeEnabled: Boolean = false,
        bridgeHost: String = "",
    ) {
        teardown()
        this.serverMode = serverMode
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
        // Mirror the transport's state into the controller's stream. On a hub
        // (A-side) we additionally fold the bridge MeshClient's liveness into the
        // Hub state so the notification shows the bridge as up/down/reconnecting.
        // When there is no bridge, foldBridge is a no-op and the primary state is
        // published verbatim (zero regression).
        stateMirrorJob = scope.launch {
            t.state.collect {
                lastPrimaryState = it
                _state.value = foldBridge(it)
            }
        }
        t.start()

        // Bridge: a hub-only secondary link to a second hub. Default OFF means
        // this branch is skipped and `bridge` stays null.
        if (serverMode && bridgeEnabled && bridgeHost.isNotBlank()) {
            val b = MeshClient(
                host = bridgeHost,
                events = this,
                role = MeshClient.Role.BRIDGE,
                deviceId = settings.deviceId(),
            )
            bridge = b
            // Reflect the bridge link's own connect/disconnect/reconnect into the
            // controller's Hub state. Re-published whenever EITHER the primary
            // (peer count) or the bridge link changes.
            bridgeStateJob = scope.launch {
                b.state.collect {
                    lastBridgeState = it
                    _state.value = foldBridge(lastPrimaryState)
                }
            }
            b.start()
            Log.i(TAG, "Bridge enabled -> $bridgeHost")
        }
        Log.i(TAG, "Configured serverMode=$serverMode host='$host' bridge=${bridge != null}")
    }

    // ---- MeshEvents ------------------------------------------------------

    /**
     * Persist an inbound peer message. Returns true if it was new.
     *
     * Called from the hub's [MeshServer] reader for a LEAF-origin message. As
     * well as persisting, a hub with a live [bridge] forwards it to the second
     * hub (the A->B direction). The far hub's own [MeshServer] handles the
     * reverse (B->A) so the two forwarding paths never both fire on one hub.
     * Leaf-origin frames are hop 0 / no origin, so the bridge stamps hop 1.
     */
    override suspend fun onMessage(message: Message): Boolean {
        val isNew = repository.acceptRemote(message)
        forwardToBridge(message, hop = 0, originId = null)
        return isNew
    }

    /**
     * Unified inbound for a [MeshClient]: a PRIMARY leaf just persists; a BRIDGE
     * link persists AND fans the far-hub message out to this hub's local leaves.
     */
    override suspend fun ingest(
        message: Message,
        hop: Int,
        originId: String?,
        fromBridge: Boolean,
    ) {
        if (!fromBridge) {
            // PRIMARY leaf: the hub owns fan-out, a leaf only stores.
            repository.acceptRemote(message)
            return
        }
        // BRIDGE inbound: persist, then relay to our local leaves via the
        // primary transport (the hub's MeshServer). We deliberately do NOT push
        // it back across the bridge it arrived on; the seen-set would refuse it
        // anyway, but excluding the inbound edge is the structural guard.
        val isNew = repository.acceptRemote(message)
        if (isNew) transport?.send(message)
        else Log.d(TAG, "Duplicate ${message.id} from bridge; not re-fanned")
    }

    /**
     * A->B forwarding chokepoint. No-op unless this hub has a live [bridge].
     * Asks the loop-prevention policy whether (and at what hop) to cross; on a
     * non-null answer, frames the message onto the bridge with hop+1 and an
     * origin stamp. Shared by [onMessage] (leaf-origin) and [sendLocal]
     * (self-origin) so every locally-visible message reaches the second hub once.
     */
    private suspend fun forwardToBridge(message: Message, hop: Int, originId: String?) {
        val b = bridge ?: return
        val nextHop = bridgePolicy.bridgeHopFor(message.id, hop) ?: return
        val stampedOrigin = originId ?: message.senderId
        b.sendRaw(MessageWire.encode(message, hop = nextHop, originId = stampedOrigin))
    }

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
        if (transport === bridge) {
            // The bridge (re)connected. Partition recovery, B->A direction: ask
            // the second hub for everything we missed while the bridge was down
            // and replay it locally. We send ONLY sync_req here — never the
            // outbox: a hub's LOCAL rows belong to its own leaves and must not
            // spill onto the second hub. (A->B recovery is symmetric: the far
            // hub's MeshServer sends us a sync_req on our opening bridge_hello,
            // which our BRIDGE client answers — see MeshClient/MeshServer.)
            // Bridge backfill arriving here lands via ingest(fromBridge=true), so
            // the seen-set + DAO dedup prevent any echo or duplicate.
            transport.sendRaw(
                MessageWire.encodeSyncReq(repository.latestCreatedAt()),
            )
            return
        }
        if (transport is MeshClient) {
            val since = repository.latestCreatedAt()
            transport.sendRaw(MessageWire.encodeSyncReq(since))
        }
        flushOutbox()
    }

    /**
     * Stage-1 bridge partition recovery: our backfill high-water mark, shared by
     * BOTH recovery directions (the far hub asks us for this via sync_req on the
     * bridge; we ask the far hub for ours in [onLinkEstablished]).
     */
    override suspend fun bridgeWatermark(): Long = repository.latestCreatedAt()

    /**
     * Fold the A-side bridge MeshClient's liveness into a [MeshState.Hub] so the
     * notification can show the inter-hub bridge as up / down / reconnecting.
     *
     * Zero-regression contract: when there is NO bridge configured ([bridge] is
     * null) this returns [primary] untouched — so a plain hub publishes
     * `Hub(peerCount)` with `bridge = null`, byte-for-byte the pre-bridge state.
     * It only ever annotates a [MeshState.Hub] (the device is the hub that owns
     * the outbound bridge); any other state passes through unchanged.
     */
    private fun foldBridge(primary: MeshState): MeshState {
        if (bridge == null) return primary
        if (primary !is MeshState.Hub) return primary
        val up = lastBridgeState is MeshState.Connected
        return primary.copy(bridge = up)
    }

    // ---- Layer 4: AI summary (server-only) -------------------------------

    /**
     * A device asked for a situational summary.
     *
     * Server-only: only the hub holds the model and the authoritative chat
     * history, so only the hub generates. A non-hub (leaf) ignores it — the
     * requesting leaf reaches the hub because the hub relays nothing for
     * summary_req; it is the hub's reader that invokes this.
     *
     * Returns immediately: the heavyweight generation is launched on
     * [summaryDispatcher] so the transport reader (and thus plain-chat relay)
     * is never blocked. [summaryInFlight] coalesces duplicate requests so a
     * room full of devices tapping the button does not queue N generations.
     */
    override suspend fun onSummaryRequest(since: Long) {
        if (!serverMode) {
            Log.d(TAG, "Ignoring summary_req: not the hub")
            return
        }
        if (!summaryInFlight.compareAndSet(false, true)) {
            Log.i(TAG, "Summary already in flight; coalescing request")
            return
        }
        // Detach from the reader: generation must not hold the read loop.
        // Tracked in summaryJob so teardown() can cancel a stale generation.
        summaryJob = scope.launch(summaryDispatcher) {
            _summaryRunning.value = true
            try {
                val engine = resolveEngine()
                if (engine == null) {
                    Log.w(TAG, "No summarization engine; dropping summary_req")
                    _summaryError.value = "要約エンジンが利用できません"
                    return@launch
                }
                val window = repository.recentForSummary(
                    SummarizationEngine.MAX_SUMMARY_MESSAGES,
                )
                Log.i(TAG, "Summarising ${window.size} message(s)")
                val text = engine.summarize(window)
                val summary = MessageWire.aiSummaryMessage(
                    id = UUID.randomUUID().toString(),
                    body = text,
                    createdAt = System.currentTimeMillis(),
                )
                // Persist on the hub, then relay to every leaf. Leaves dedup
                // via the DAO's OnConflict.IGNORE just like any peer message.
                repository.acceptAiSummary(summary)
                transport?.send(summary)
                // Also cross to the second hub so its leaves see the summary.
                forwardToBridge(summary, hop = 0, originId = null)
                Log.i(TAG, "AI summary ${summary.id} stored + relayed")
            } catch (e: CancellationException) {
                // A reconfigure/stop cancelled us: expected, not an error.
                Log.i(TAG, "Summary generation cancelled (reconfigure)")
                throw e
            } catch (e: Exception) {
                Log.w(TAG, "Summary generation failed: ${e.message}")
                _summaryError.value = "要約の生成に失敗しました（モデル読込/推論エラー）"
            } finally {
                _summaryRunning.value = false
                summaryInFlight.set(false)
            }
        }
    }

    /**
     * UI entry point for the "状況まとめ" action.
     *
     * Hub: generate locally (straight into [onSummaryRequest]).
     * Leaf: frame a summary_req onto the socket so the hub generates and the
     * result comes back as a normal relayed AI message.
     *
     * @return false if a leaf has no live link to the hub (UI shows
     *   "サーバー未接続"); the model is never run leaf-side.
     */
    suspend fun requestSummary(): Boolean {
        if (serverMode) {
            onSummaryRequest(0L)
            return true
        }
        val t = transport ?: run {
            Log.i(TAG, "requestSummary: no transport (server not connected)")
            return false
        }
        return t.sendRaw(MessageWire.encodeSummaryReq())
    }

    private fun resolveEngine(): SummarizationEngine? {
        resolvedEngine?.let { return it }
        val built = summarizationEngineProvider?.invoke() ?: return null
        resolvedEngine = built
        return built
    }

    // ---- Stage-1 bridge: forwarding policy (loop prevention) -------------

    /**
     * Loop-prevention policy for the future 2-hub bridge. Pure state machine,
     * owned by the controller but kept as its own object so it has no Room /
     * Context dependency and can be unit-tested directly. PR#3's relay will ask
     * [BridgePolicy.bridgeHopFor] before forwarding a message across a bridge.
     *
     * PR#3 wires this into the hub's relay via [bridgeHopFor]: a chat message
     * is only forwarded onto a *bridge* connection when the policy allows it.
     * Leaf fan-out never consults it, so the star topology is unchanged. No
     * bridge connection is created until PR#4 wires the inter-hub transport, so
     * in this PR the bridge branch is exercised only by tests.
     */
    val bridgePolicy = BridgePolicy()

    /**
     * Stage-1 bridge forwarding decision, delegated to [bridgePolicy]. Called
     * by the hub's reader (on Dispatchers.IO, possibly concurrently across
     * client readers); [BridgePolicy.bridgeHopFor] is @Synchronized so the
     * read-modify-write of its seen-set is safe.
     */
    override fun bridgeHopFor(messageId: String, hop: Int): Int? =
        bridgePolicy.bridgeHopFor(messageId, hop)

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
        // A self-authored message also crosses to the second hub (hop 0 -> 1).
        // No-op when no bridge is configured.
        forwardToBridge(message, hop = 0, originId = null)
    }

    private suspend fun teardown() {
        stateMirrorJob?.cancel()
        stateMirrorJob = null
        bridgeStateJob?.cancel()
        bridgeStateJob = null
        lastPrimaryState = MeshState.Idle
        lastBridgeState = MeshState.Idle
        // Per-request scope: a generation tied to the old transport must not
        // outlive it. Cancel it before dropping the socket so it cannot relay
        // onto a torn-down transport or leak across a mode/host switch.
        summaryJob?.cancel()
        summaryJob = null
        // Defensive: if the cancel raced past the coroutine's finally, clear
        // the single-flight latch so the next mode still accepts a request.
        summaryInFlight.set(false)
        _summaryRunning.value = false
        // Idle release: if this device is no longer the hub it will not run
        // the LFM again until reconfigured, so free its native memory now.
        if (serverMode) {
            resolvedEngine?.let { engine ->
                runCatching { engine.release() }
                    .onFailure { Log.w(TAG, "Engine release failed: ${it.message}") }
            }
        }
        transport?.stop()
        transport = null
        // Tear the bridge down too so a mode/host/bridge change cannot leave a
        // stale inter-hub socket forwarding onto a torn-down transport.
        bridge?.stop()
        bridge = null
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

/**
 * Stage-1 bridge forwarding policy: stops a message from ping-ponging across an
 * inter-hub bridge forever. Two independent guards:
 *
 *  1. **Seen-set (primary loop break)** — a bounded set (FIFO eviction) of
 *     message ids THIS device has already forwarded across the bridge. A second
 *     appearance of the same id is refused, severing the A->B->A echo.
 *  2. **[MAX_HOP] ceiling (belt-and-braces)** — a hard cap on bridge crossings
 *     so even a dedup miss cannot let hop run away.
 *
 * NOTE — this is NOT the DAO's OnConflict.IGNORE dedup. That dedup stops a
 * *duplicate store* (an id is never persisted twice). This seen-set stops a
 * *re-forward across the bridge* (an id is never sent over the bridge twice).
 * Different resources; neither subsumes the other.
 *
 * Pure logic with no Room / Context dependency, so it is unit-tested directly.
 * [capacity] is injectable purely so a test can drive eviction with a tiny
 * bound; production uses [DEFAULT_CAPACITY].
 *
 * Thread-safety: PR#3's relay calls [bridgeHopFor] from each client's reader
 * coroutine running on [Dispatchers.IO]. Multiple readers can run concurrently
 * on different threads, so mutual exclusion is required. [Synchronized] is a
 * non-suspending, blocking mutual-exclusion primitive — the right fit for
 * guarding the non-thread-safe [LinkedHashMap] below, since this is a simple
 * read-modify-write with no suspension points (more natural than a [Mutex]).
 */
class BridgePolicy(private val capacity: Int = DEFAULT_CAPACITY) {

    // Bounded FIFO of forwarded ids: removeEldestEntry drops the oldest once
    // capacity is exceeded. Each id is put at most once (a repeat is refused
    // before the put), so insertion order is the eviction order.
    private val seen = object : LinkedHashMap<String, Boolean>(16, 0.75f, false) {
        override fun removeEldestEntry(eldest: Map.Entry<String, Boolean>): Boolean =
            size > capacity
    }

    /**
     * Decide whether to forward [messageId] across a bridge, and at what hop.
     * Pure decision plus one side effect: recording the id in the seen-set. It
     * never touches the transport. PR#3's relay calls this and, on a non-null
     * result, frames the message onto the bridge stamped with the returned hop.
     *
     * @return [hop] + 1 (the hop to stamp on the forwarded frame), or null to
     *   NOT forward — either because this id was already forwarded (loop break)
     *   or because [hop] has reached [MAX_HOP] (ceiling guard).
     */
    @Synchronized
    fun bridgeHopFor(messageId: String, hop: Int): Int? {
        if (seen.containsKey(messageId)) return null
        if (hop >= MAX_HOP) return null
        seen[messageId] = true
        return hop + 1
    }

    companion object {
        /**
         * Maximum bridge crossings a message may make. In a 2-hub bridge a
         * legitimate message crosses at most once (hop 0 -> 1), so a ceiling of
         * 2 is a generous safety net behind the seen-set. Lives here (not on the
         * wire) because it is a forwarding-policy decision, not frame format.
         */
        const val MAX_HOP = 2

        /**
         * Default seen-set capacity. Bounds memory to this many recently
         * forwarded ids; older ones fall out. An evicted id could in theory be
         * re-forwarded, but only long after any in-flight echo could still be
         * circulating, so it is harmless for loop control.
         */
        const val DEFAULT_CAPACITY = 2048
    }
}
