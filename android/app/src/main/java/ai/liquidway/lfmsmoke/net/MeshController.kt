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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

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
    /**
     * Operator-layer 3: how long a hub that is NOT the dispatch target waits
     * before it falls back to claiming a question itself. During this window the
     * target (which claims with zero delay) normally claims first, so the
     * non-target stands down on the arriving `summary_claim` — that is the load
     * balancing. If the target is dead and never claims, the non-target claims
     * after this window so the answer is never lost (liveness). Injectable so a
     * test can drive it to 0 (immediate) or a small value deterministically;
     * production uses [DEFAULT_DISPATCH_FALLBACK_MS].
     */
    private val dispatchFallbackMs: Long = DEFAULT_DISPATCH_FALLBACK_MS,
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
     * Operator-layer: total summaries this hub has produced. Incremented once per
     * completed generation and piggybacked on the bridge heartbeat as
     * [HubLoad.dispatchCount] so peers can see this hub's lifetime workload. A
     * monotonic counter feeding future load-based dispatch hints (later PR).
     */
    private val dispatchCount = AtomicInteger(0)

    /**
     * Operator-layer: the latest load each peer hub reported on its bridge
     * heartbeat, keyed by deviceId. Updated from the transport reader threads
     * (so a [ConcurrentHashMap]); exposed read-only via [peerLoadView] for a
     * later PR to base operator selection on. Empty on a single hub (no bridge,
     * no hellos) — the zero-regression case.
     */
    private val peerLoads = ConcurrentHashMap<String, PeerLoad>()

    /**
     * A peer hub's last-reported load plus when we heard it. [seenAt] lets a
     * later PR age out a silent peer (a hub whose heartbeat stopped) instead of
     * trusting stale load forever.
     */
    data class PeerLoad(val load: HubLoad, val seenAt: Long)

    /**
     * Read-only snapshot of every peer hub's last-reported load. A defensive copy
     * so a caller cannot mutate the live map. Consumed by [recomputeOperator];
     * also kept public for tests/diagnostics.
     */
    fun peerLoadView(): Map<String, PeerLoad> = peerLoads.toMap()

    /**
     * Operator-layer 2: the deterministic, vote-free operator election (pure
     * logic, see [OperatorElection]). Fed the same gossiped load picture every
     * hub sees, so all hubs independently compute the same operator with no
     * consensus protocol — split-brain-free by construction. Recomputed on every
     * peer heartbeat via [recomputeOperator].
     */
    private val operatorElection = OperatorElection()

    private val _effectiveOperatorId = MutableStateFlow<String?>(null)

    /**
     * Operator-layer 2 OUTPUT — observable only, no behaviour attached. The
     * deviceId of the hub all hubs have agreed is the operator (after
     * hysteresis), or null before the first election. PR#9 will consume this to
     * route/dispatch; THIS PR must not let it change any routing or summary
     * ownership. Null/self-only on a single hub (no peers), where it is inert.
     */
    val effectiveOperatorId: StateFlow<String?> = _effectiveOperatorId.asStateFlow()

    private val _amIOperator = MutableStateFlow(false)

    /**
     * Operator-layer 2 OUTPUT — true when this hub is the elected operator.
     * Derived from [effectiveOperatorId] == self. Observable only this PR (no
     * action taken on it). On a single hub the lone candidate is self, so once an
     * election has run this is trivially true — but with nothing acting on it,
     * routing is unchanged (zero regression).
     */
    val amIOperator: StateFlow<Boolean> = _amIOperator.asStateFlow()

    /**
     * Operator-layer 3: the freshest advisory dispatch hint received from the
     * operator over a bridge_hello — which hub it wants to claim a new summary
     * preferentially, and when we heard it. Null target / null holder means no
     * usable hint (operator absent or never seen). [seenAt] ages the hint out so
     * a dead operator's stale hint cannot pin claim timing forever — once stale,
     * the controller falls straight back to the PR#6 per-request-ownership path.
     */
    @Volatile
    private var dispatchHint: DispatchHint? = null

    /** A received dispatch hint plus when it arrived (for staleness aging). */
    private data class DispatchHint(val target: String?, val seenAt: Long)

    /**
     * Operator-layer 3: the dispatch target THIS hub should bias its claim
     * timing toward, or null to fall back to the PR#6 floor.
     *
     *  - If this hub IS the operator, it uses its OWN freshly-computed target
     *    ([localDispatchTarget]); the operator never relies on a received hint
     *    (it does not hear its own). Mirrors how the operator advertises.
     *  - Otherwise it uses the freshest hint a peer (the operator) advertised,
     *    provided it is within the stale window (the same window the election
     *    uses, so a quiet operator ages out of both consistently).
     *
     * Either way this is purely a timing input — the claim CAS is the floor.
     */
    private fun effectiveDispatchTarget(now: Long): String? {
        if (_amIOperator.value) return localDispatchTarget()
        val h = dispatchHint ?: return null
        if (now - h.seenAt > OperatorElection.DEFAULT_PEER_STALE_MS) return null
        return h.target
    }

    /**
     * Operator-layer 2: recompute the operator from the current load picture and
     * publish it. Called after a peer heartbeat updates [peerLoads]. Pure
     * computation + StateFlow publish — it deliberately triggers NO routing or
     * ownership change (that is PR#9). On a single hub the candidate set is just
     * self, so this commits self and stays inert.
     */
    private fun recomputeOperator() {
        val self = cachedSelfId ?: return // not a bridged hub yet -> no election
        val op = operatorElection.elect(
            selfId = self,
            selfLoad = localLoad(),
            peers = peerLoads,
            now = System.currentTimeMillis(),
        )
        _effectiveOperatorId.value = op
        _amIOperator.value = (op == self)
        // Re-publish the Hub state so the (bridge-only) operator label in the
        // notification tracks the new operator. foldBridge is a no-op without a
        // bridge, so a plain hub's published state is untouched (zero regression).
        if (bridge != null) _state.value = foldBridge(lastPrimaryState)
    }

    /**
     * Layer-6 per-question ownership: lets a `summary_req` cross the bridge so
     * EITHER hub can answer, while the claim CAS keeps the common case to a
     * single answer (and a simultaneous cross-ack to at most two). Pure logic,
     * no Room/Context dependency, so it is unit-tested directly. Idle on a
     * single hub (bridge==null): the request never crosses and only the local
     * hub claims, so the behaviour is byte-for-byte the pre-layer-6 path.
     */
    val questionOwnership = QuestionOwnership()

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
            // Resolve + cache our deviceId now so the MeshServer's echoed hello
            // (operator-layer load) can stamp it without suspending on the
            // reader path (see [localDeviceId]).
            val selfDeviceId = settings.deviceId().also { cachedSelfId = it }
            val b = MeshClient(
                host = bridgeHost,
                events = this,
                role = MeshClient.Role.BRIDGE,
                deviceId = selfDeviceId,
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
     * Operator-layer: this hub's current load, stamped onto every outgoing
     * bridge_hello. [HubLoad.queueDepth] is derived from [summaryInFlight] — with
     * single-flight summarisation the summariser is either idle (0) or busy (1),
     * which is the honest "current pressure" signal without inventing a queue
     * that does not exist. [HubLoad.dispatchCount] is the lifetime total.
     */
    override fun localLoad(): HubLoad = HubLoad(
        queueDepth = if (summaryInFlight.get()) 1 else 0,
        dispatchCount = dispatchCount.get(),
    )

    /**
     * Operator-layer: record a peer hub's load reported on its bridge heartbeat.
     * Read-only view today (no routing consumes it); a later PR uses it for
     * load-based operator selection. Overwrites with the latest report + a fresh
     * timestamp so a silent peer can be aged out later.
     */
    override suspend fun onPeerLoad(deviceId: String, load: HubLoad) {
        peerLoads[deviceId] = PeerLoad(load, System.currentTimeMillis())
        Log.d(TAG, "Peer $deviceId load: queue=${load.queueDepth} dispatched=${load.dispatchCount}")
        // Operator-layer 2: refresh the elected operator from the new picture.
        recomputeOperator()
    }

    /**
     * Operator-layer 3: the advisory dispatch hint this hub advertises on its
     * outgoing/echoed bridge_hello. Non-null ONLY when this hub is the elected
     * operator AND there is a live peer to point at — then it is the least-loaded
     * hub ([DispatchTarget]). A non-operator hub (or a lone operator with no
     * peers) advertises null, so a non-operator never steers dispatch and a
     * single hub emits nothing extra (zero regression). Computed fresh per hello
     * from the current gossiped picture, mirroring how the election is computed.
     */
    override fun localDispatchTarget(): String? {
        if (!_amIOperator.value) return null
        val self = cachedSelfId ?: return null
        return DispatchTarget.choose(
            selfId = self,
            selfLoad = localLoad(),
            peers = peerLoads,
            now = System.currentTimeMillis(),
        )
    }

    /**
     * Operator-layer 3: record the freshest dispatch hint a peer (the operator)
     * advertised on its bridge_hello. Stored with a timestamp so it ages out;
     * consumed only to bias claim TIMING in [onSummaryRequest] — never to change
     * who ends up owning a question (that is the claim CAS). On a single hub no
     * hello ever arrives, so this is never called and the hint stays null.
     */
    override suspend fun onDispatchHint(fromDeviceId: String, target: String?) {
        // Only the elected operator's hint counts. A non-operator hello (which
        // carries a null target) must NOT clobber the operator's live hint — that
        // would erase the bias every heartbeat. Until the first election names an
        // operator we accept any hint (bootstrap); thereafter we gate on it.
        val op = _effectiveOperatorId.value
        if (op != null && fromDeviceId != op) {
            Log.d(TAG, "Ignoring dispatch hint from non-operator $fromDeviceId")
            return
        }
        dispatchHint = DispatchHint(target, System.currentTimeMillis())
        Log.d(TAG, "Dispatch hint from $fromDeviceId -> target=$target")
    }

    /**
     * Operator-layer: this hub's deviceId for the [MeshServer]'s echoed hello.
     * Non-suspending (the reader path cannot suspend cheaply), so it reads the
     * id cached by [configure]/[selfId]; falls back to "hub" only before the
     * first resolution (a bridged hub caches it on configure).
     */
    override fun localDeviceId(): String = cachedSelfId ?: "hub"

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
        return primary.copy(bridge = up, operatorLabel = operatorLabel())
    }

    /**
     * Operator-layer 2: a plain readout of the elected operator for the
     * notification — "自分" when this hub is the operator, "peer:xxxx" (short id)
     * otherwise, or null before the first election. Bridge-only by construction:
     * only [foldBridge] (which is a no-op without a bridge) reads it, so a plain
     * hub never shows it (zero regression). Informational only — no behaviour.
     */
    private fun operatorLabel(): String? {
        val op = _effectiveOperatorId.value ?: return null
        return if (op == cachedSelfId) "自分" else "peer:${op.take(6)}"
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
    override suspend fun onSummaryRequest(since: Long, questionId: String?) {
        if (!serverMode) {
            Log.d(TAG, "Ignoring summary_req: not the hub")
            return
        }
        // Resolve the correlation id. A modern leaf stamps one; a legacy leaf
        // sends none, so the FIRST receiving hub mints a fallback. NOTE: with a
        // legacy (id-less) leaf in the mix the two hubs mint *different* ids for
        // the same tap and cannot recognise it as one question -> a double
        // answer can occur. Known Stage-1 limitation (modern leaves are fine).
        val qid = questionId ?: UUID.randomUUID().toString()

        val self = selfId()

        // Operator-layer 3: advisory dispatch — bias WHEN we attempt the claim,
        // never WHETHER the question stays single-owned (the CAS in
        // [claimAndGenerate] is the floor). Three cases:
        //  - no fresh hint (operator absent/stale) -> PR#6 path: claim NOW
        //    (claim precedes the forwarded request on the same ordered socket,
        //    so the far hub stands down -> single owner, exactly as PR#6).
        //  - fresh hint and WE are the target      -> claim NOW (we are the
        //    chosen, idlest hub); same PR#6 claim-first ordering.
        //  - fresh hint and we are NOT the target  -> forward the request to the
        //    far hub FIRST (so the target hub actually receives it and can
        //    claim), THEN defer OUR claim by [dispatchFallbackMs]. Normally the
        //    target claims first and its summary_claim makes our deferred
        //    tryClaim stand down (load balanced). If the target is dead and never
        //    claims, our deferred attempt still wins after the window, so the
        //    answer is never lost (liveness). On a single hub the hint is always
        //    null, so this branch is never taken (zero regression).
        val target = effectiveDispatchTarget(System.currentTimeMillis())
        if (target != null && target != self) {
            Log.i(TAG, "summary_req $qid: target=$target is not us; deferring ${dispatchFallbackMs}ms")
            // Hand the request to the far hub so the target can claim it. We do
            // NOT claim or announce here — that is the whole point of deferring.
            bridge?.let { b ->
                if (questionOwnership.shouldForward(qid)) {
                    b.sendRaw(MessageWire.encodeSummaryReq(since, qid))
                }
            }
            scope.launch {
                kotlinx.coroutines.delay(dispatchFallbackMs)
                claimAndGenerate(since, qid, self)
            }
            return
        }
        claimAndGenerate(since, qid, self)
    }

    /**
     * The claim + cross-bridge announce + (single-flight) generation, shared by
     * the immediate and the deferred dispatch paths. The claim CAS here is the
     * correctness floor: regardless of how dispatch timing biased our arrival, a
     * foreign owner means we stand down, so a question stays single-owned (a
     * simultaneous cross-ack to at most two; a third suppressed).
     */
    private suspend fun claimAndGenerate(since: Long, qid: String, self: String) {
        // Per-question ownership CAS. If a peer hub's claim already arrived we
        // stand down here (the single-owner common case — and exactly how a
        // deferred non-target stands down once the target has claimed). On a
        // single hub this always wins (no peer can have claimed), so behaviour
        // is unchanged.
        if (!questionOwnership.tryClaim(qid, self)) {
            Log.i(TAG, "summary_req $qid already owned by a peer; standing down")
            return
        }
        // We own this question. Announce the claim across the bridge BEFORE we
        // forward the request, so the far hub records our ownership and stands
        // down when it then reads the forwarded request (claim + request travel
        // the same ordered bridge socket, so claim-first guarantees the far hub
        // sees it first). A genuinely simultaneous claim (both hubs originate
        // before either's claim lands) crosses in flight -> both answer
        // (two-bubble cross-ack), handled in [onSummaryClaim]. shouldForward is
        // the forward-once guard, so a request the deferred path already
        // forwarded is not sent twice.
        bridge?.let { b ->
            b.sendRaw(MessageWire.encodeSummaryClaim(qid, self))
            if (questionOwnership.shouldForward(qid)) {
                b.sendRaw(MessageWire.encodeSummaryReq(since, qid))
            }
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
                // Operator-layer: count this completed dispatch so the next
                // heartbeat advertises the updated lifetime workload.
                dispatchCount.incrementAndGet()
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
        // Mint the correlation id at the request source so BOTH hubs see ONE
        // question id and per-request ownership can dedup the answer to one.
        val questionId = UUID.randomUUID().toString()
        if (serverMode) {
            onSummaryRequest(0L, questionId)
            return true
        }
        val t = transport ?: run {
            Log.i(TAG, "requestSummary: no transport (server not connected)")
            return false
        }
        return t.sendRaw(MessageWire.encodeSummaryReq(questionId = questionId))
    }

    /**
     * Stage-1 bridge: a peer hub claimed [questionId]. Record it so our own
     * pending generation for that question stands down (single-owner case). If
     * we had already won and started, the claims crossed in flight ("すれ違い"):
     * both hubs answer (two AI bubbles) and we log the dual-option outcome. We
     * never cancel a generation already begun. Leaf/non-hub ignores it.
     */
    override suspend fun onSummaryClaim(questionId: String, ownerId: String) {
        if (!serverMode) return
        if (ownerId == selfId()) return // our own claim echoed back; ignore
        questionOwnership.onRemoteClaim(questionId, ownerId)
        if (questionOwnership.isCrossAck(questionId)) {
            Log.i(TAG, "Cross-ack on $questionId (claim from $ownerId): both hubs answer, 2 options")
        } else {
            Log.i(TAG, "Peer $ownerId claimed $questionId; this hub stands down")
        }
    }

    // This hub's stable id, cached so the claim path need not suspend on every
    // request. Falls back to "hub" before the first resolution (rare; the id is
    // resolved on configure of a bridged hub).
    @Volatile
    private var cachedSelfId: String? = null

    private suspend fun selfId(): String =
        cachedSelfId ?: settings.deviceId().also { cachedSelfId = it }

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
        // Operator-layer 2: drop the gossiped picture and the elected operator so
        // a reconfigure starts a fresh election (a stale peer/operator must not
        // survive a mode/host/bridge switch).
        peerLoads.clear()
        operatorElection.reset()
        _effectiveOperatorId.value = null
        _amIOperator.value = false
        // Operator-layer 3: drop any dispatch hint so a reconfigure starts on the
        // PR#6 floor until a fresh operator advertises again.
        dispatchHint = null
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

        /**
         * Operator-layer 3: default fallback delay a NON-target hub waits before
         * claiming a question itself. Long enough that the chosen target (which
         * claims immediately) and its `summary_claim` normally reach this hub
         * first — so the non-target stands down and load is balanced — yet short
         * enough that a dead target only delays the answer briefly before another
         * hub claims (liveness). Comfortably under a user's patience for a summary.
         */
        const val DEFAULT_DISPATCH_FALLBACK_MS = 600L

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

/**
 * Stage-1 bridge: per-question single-ownership of an AI summary (layer 6).
 *
 * A `summary_req` carries a `questionId`; with the request now crossing the
 * bridge so EITHER hub can answer, two hubs could both generate. This registry
 * gives a leaderless, per-request owner so that — in the common case — exactly
 * one hub answers a given question:
 *
 *  1. **Forward-once** ([shouldForward]) — a hub forwards a given questionId
 *     across the bridge at most once, so a `summary_req` does not ping-pong.
 *  2. **Claim CAS** ([tryClaim]) — a hub may generate for a questionId only if
 *     no *foreign* owner has already been recorded for it. The first hub to
 *     claim wins; a hub that has already seen a peer's claim stands down.
 *  3. **Cross-ack ("すれ違い")** — when two hubs claim almost simultaneously
 *     their claims cross in flight: each had already locally won [tryClaim]
 *     before the other's claim arrived. [onRemoteClaim] then records the peer
 *     as a co-owner (it does NOT retract a generation already in flight), so
 *     BOTH answer and the leaf shows two AI bubbles — the deliberate
 *     "two-options" outcome. [isCrossAck] reports this so the caller can log it.
 *  4. **Runaway guard** — at most [maxOwners] distinct owners are ever recorded
 *     per question, so a third (or later) hub is suppressed: it can neither
 *     [tryClaim] (a foreign owner is present) nor be admitted by [onRemoteClaim]
 *     once the cap is reached.
 *
 * SECURITY NOTE: `ownerId` is the peer's *self-report* and is not authenticated.
 * A malicious peer could claim every questionId to suppress real answers (a DoS)
 * or spoof an ownerId. Stage-1 assumes a same-LAN, operator-configured host, so
 * this is accepted; ownership/identity verification is future work (Task #21).
 *
 * Thread-safety: like [BridgePolicy], every method is [Synchronized] — the hub's
 * reader coroutines (and the bridge client's reader) call in concurrently, and
 * each operation is a short read-modify-write over the non-thread-safe maps.
 */
class QuestionOwnership(
    private val maxOwners: Int = DEFAULT_MAX_OWNERS,
    private val capacity: Int = DEFAULT_CAPACITY,
) {
    // questionId -> ordered set of ownerIds that have claimed it (capped at
    // maxOwners). LinkedHashSet preserves "first claim wins" ordering for logs.
    private val owners = boundedMap<MutableSet<String>>()

    // questionIds already forwarded across the bridge (forward-once guard).
    private val forwarded = boundedMap<Boolean>()

    /** True the FIRST time this questionId is offered for cross-bridge forward. */
    @Synchronized
    fun shouldForward(questionId: String): Boolean {
        if (forwarded.containsKey(questionId)) return false
        forwarded[questionId] = true
        return true
    }

    /**
     * Attempt to own [questionId] as [ownerId]. Succeeds (recording the owner)
     * only when no FOREIGN owner is yet present — i.e. the question is unclaimed
     * or already claimed by this same owner (idempotent re-entry). A hub that has
     * already recorded a peer's claim therefore stands down (returns false).
     *
     * @return true if this owner may proceed to generate.
     */
    @Synchronized
    fun tryClaim(questionId: String, ownerId: String): Boolean {
        val set = owners[questionId]
        if (set == null) {
            owners[questionId] = linkedSetOf(ownerId)
            return true
        }
        if (set.contains(ownerId)) return true // idempotent: already mine
        // A foreign owner is present -> stand down (normal single-owner case).
        return false
    }

    /**
     * Record a peer's [ownerId] claim for [questionId]. Used purely to make a
     * later local [tryClaim] stand down (normal case) and to admit a co-owner up
     * to the [maxOwners] cap (cross-ack case). Never cancels work already begun.
     */
    @Synchronized
    fun onRemoteClaim(questionId: String, ownerId: String) {
        val set = owners.getOrPut(questionId) { linkedSetOf() }
        if (set.contains(ownerId)) return
        if (set.size >= maxOwners) return // runaway guard: cap distinct owners
        set.add(ownerId)
    }

    /**
     * True when [questionId] now has more than one distinct owner — the
     * cross-ack ("すれ違い") outcome where both hubs answer. Caller uses it only
     * to emit the dual-answer log line.
     */
    @Synchronized
    fun isCrossAck(questionId: String): Boolean =
        (owners[questionId]?.size ?: 0) > 1

    private fun <V> boundedMap(): LinkedHashMap<String, V> =
        object : LinkedHashMap<String, V>(16, 0.75f, false) {
            override fun removeEldestEntry(eldest: Map.Entry<String, V>): Boolean =
                size > capacity
        }

    companion object {
        /**
         * Distinct owners allowed per question. Two is the deliberate ceiling:
         * a clean single owner in the common case, at most a two-bubble
         * cross-ack on a simultaneous claim, never a third runaway answer.
         */
        const val DEFAULT_MAX_OWNERS = 2

        /** Bounds memory to this many recently-seen questionIds (FIFO eviction). */
        const val DEFAULT_CAPACITY = 2048
    }
}
