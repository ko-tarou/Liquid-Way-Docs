package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The star-topology hub (serverMode = ON).
 *
 * Responsibilities:
 *  - accept many client sockets, one accept-loop coroutine,
 *  - one reader coroutine per client,
 *  - on inbound msg: persist via [events] then relay to *every other* client,
 *  - on inbound sync_req: replay backfill to *that one* client only,
 *  - on local [send]: fan out to *all* clients.
 *
 * Writes to a socket are serialised per-connection by synchronising on its
 * [OutputStream]; reads are independent so a slow/dead client cannot block the
 * accept loop or other peers.
 */
class MeshServer(
    private val port: Int = MessageWire.DEFAULT_PORT,
    /**
     * Interface to bind. Production uses the IPv4 wildcard "0.0.0.0" so LAN
     * peers (192.168.x.x) can reach the hub; the explicit IPv4 address is
     * required because the no-arg InetSocketAddress(port) resolves to the IPv6
     * wildcard "[::]" on some Android images, which refuses IPv4 peers. The
     * deterministic relay tests pass "127.0.0.1" to stay on the loopback.
     */
    private val bindAddress: String = "0.0.0.0",
    private val events: MeshEvents,
) : MeshTransport {

    private companion object {
        const val TAG = "LiqMesh/Server"
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val connId = AtomicLong(0)
    private val clients = ConcurrentHashMap<Long, Connection>()

    private var serverSocket: ServerSocket? = null
    private var acceptJob: Job? = null

    /**
     * The port the hub is actually listening on. Equals [port] in production;
     * when constructed with port 0 (tests) this resolves to the OS-assigned
     * ephemeral port after [start]. -1 before binding.
     */
    val boundPort: Int
        get() = serverSocket?.localPort ?: -1

    private val _state = MutableStateFlow<MeshState>(MeshState.Idle)
    override val state: StateFlow<MeshState> = _state.asStateFlow()

    private class Connection(val id: Long, val socket: Socket) {
        val out: OutputStream = socket.getOutputStream()
        // Serialises concurrent writes (local fan-out vs. relay) to this peer.
        val writeLock = Any()

        /**
         * Stage-1 bridge: true once this peer announces itself with a
         * [MessageWire.Frame.BridgeHello] (i.e. it is another hub reached over a
         * bridge link, not a plain leaf). Flipped from the per-connection reader
         * coroutine and read from any reader during relay, so @Volatile.
         *
         * No bridge transport is wired until PR#4, so in production this stays
         * false; PR#3's tests drive it by sending a bridge_hello frame.
         */
        @Volatile
        var isBridge: Boolean = false

        /**
         * Stage-1 bridge liveness: epoch-ms of the last bridge_hello received on
         * this connection (0 = never). Refreshed by every heartbeat hello so the
         * hub can surface "bridge alive" without an extra wire type. Read/written
         * from the per-connection reader, hence @Volatile.
         */
        @Volatile
        var lastBridgeSeenAt: Long = 0L
    }

    override suspend fun start() {
        if (acceptJob != null) return
        val sock = withContext(Dispatchers.IO) {
            // Bind to an explicit IPv4 address. The no-arg
            // InetSocketAddress(port) resolves to the IPv6 wildcard "[::]" on
            // this Android image, which refuses IPv4 (127.0.0.1 / 192.168.x.x)
            // peers. LAN devices use IPv4, so an IPv4 bind is required. Bind is
            // synchronous (backlog 50) so an immediate client connect is queued
            // by the OS rather than refused.
            ServerSocket().apply {
                reuseAddress = true
                bind(
                    java.net.InetSocketAddress(
                        java.net.InetAddress.getByName(bindAddress),
                        port,
                    ),
                    50,
                )
            }
        }
        serverSocket = sock
        publishPeerCount()
        Log.i(TAG, "Listening on ${sock.localSocketAddress}")

        acceptJob = scope.launch {
            while (isActive) {
                val client = try {
                    sock.accept()
                } catch (e: Exception) {
                    if (isActive) Log.w(TAG, "accept() ended: ${e.message}")
                    break
                }
                handleClient(client)
            }
        }
    }

    private fun handleClient(socket: Socket) {
        socket.tcpNoDelay = true
        val conn = Connection(connId.incrementAndGet(), socket)
        clients[conn.id] = conn
        publishPeerCount()
        Log.i(TAG, "Client ${conn.id} connected (${clients.size} total)")

        scope.launch {
            events.onLinkEstablished(this@MeshServer)
            try {
                val reader = BufferedReader(
                    InputStreamReader(socket.getInputStream(), Charsets.UTF_8),
                )
                while (isActive) {
                    val line = reader.readLine() ?: break
                    when (val f = MessageWire.decodeFrame(line)) {
                        is MessageWire.Frame.Msg -> {
                            val isNew = events.onMessage(f.message)
                            // Relay to every *other* client. Local leaves get an
                            // unconditional fan-out (unchanged star behaviour);
                            // bridge connections get policy-gated forwarding with
                            // hop+1. The sender is always excluded.
                            relay(f.message, hop = f.hop, originId = f.originId, exclude = conn)
                            if (!isNew) Log.d(TAG, "Duplicate ${f.message.id} re-relayed")
                        }
                        is MessageWire.Frame.SyncReq -> {
                            // Backfill ONLY the requesting client. Each
                            // sync_resp is written to this conn's socket;
                            // dedup on the leaf collapses any overlap.
                            events.onSyncRequest(f.since) { line2 ->
                                writeTo(conn, line2)
                            }
                        }
                        is MessageWire.Frame.SummaryReq -> {
                            // Layer 4/6: the hub owns the model. onSummaryRequest
                            // returns fast (it dispatches generation onto its
                            // own coroutine) so this read loop keeps relaying
                            // plain chat while a summary is produced. The
                            // questionId (layer 6) lets per-request ownership
                            // dedup the answer to one across the bridge.
                            events.onSummaryRequest(f.since, f.questionId)
                        }
                        is MessageWire.Frame.BridgeHello -> {
                            // This peer is another hub reached over a bridge,
                            // not a leaf. Mark the connection so relay() routes
                            // it through the forwarding policy instead of the
                            // plain leaf fan-out. deviceId is logged only;
                            // ownership/identity checks land in a later PR.
                            conn.lastBridgeSeenAt = System.currentTimeMillis()
                            // FIRST hello only: flip to bridge AND kick partition
                            // recovery (A->B). We send sync_req(ourWatermark) back
                            // on this same connection; the far hub answers it via
                            // its BRIDGE client's onSyncRequest, replaying what we
                            // missed while partitioned. Heartbeat hellos arrive
                            // with isBridge already true, so they only refresh
                            // last-seen and do NOT re-trigger the catch-up.
                            if (!conn.isBridge) {
                                conn.isBridge = true
                                publishBridgePresence()
                                val since = events.bridgeWatermark()
                                writeTo(conn, MessageWire.encodeSyncReq(since))
                                Log.i(TAG, "Client ${conn.id} is a bridge (deviceId=${f.deviceId}); requested backfill since=$since")
                            }
                        }
                        is MessageWire.Frame.SummaryClaim -> {
                            // Layer 6: a peer hub (across the bridge) claimed a
                            // question. The controller records it so a pending
                            // local generation stands down (or, on a cross-ack,
                            // both answer).
                            //
                            // A claim is hub-to-hub control: accept it ONLY from a
                            // bridge connection. We do not honour claims from a
                            // plain leaf — an untrusted leaf forging summary_claim
                            // frames could poison every questionId's ownership and
                            // suppress all answers (a DoS). Same root cause as
                            // Task #21; ignore (log only) without dropping the link,
                            // matching the Unknown-frame tone below.
                            if (conn.isBridge) {
                                events.onSummaryClaim(f.questionId, f.ownerId)
                            } else {
                                Log.d(TAG, "Ignoring summary_claim from non-bridge client ${conn.id}")
                            }
                        }
                        // Unknown: no relay path consumes it; skip without
                        // dropping the link.
                        MessageWire.Frame.Unknown,
                        -> Unit
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Client ${conn.id} read error: ${e.message}")
            } finally {
                drop(conn)
            }
        }
    }

    /**
     * Relay an inbound chat message to every *other* connection, split by peer
     * kind:
     *
     *  - **Local leaves** (isBridge=false): unconditional fan-out of the message
     *    as-is, exactly as the star always did. This path is unchanged — no
     *    policy, no hop rewrite — so leaf↔leaf relay has zero regression.
     *  - **Bridge connections** (isBridge=true): policy-gated forwarding. The
     *    forwarding decision ([bridgeHopFor]) returns the new hop, or null to
     *    drop (already-forwarded loop break / hop ceiling). On a forward the
     *    frame is re-encoded with hop+1 and an [originId] stamp.
     *
     * The receiving connection is always excluded, so a message never echoes
     * back to its sender (this is also the bridge self-loop guard).
     *
     * SECURITY NOTE: [hop]/[originId] are the sender's self-report and are not
     * authenticated here. Loop safety rests on two independent guards in
     * [BridgePolicy] — a seen-set and a hop ceiling — not on trusting the sender.
     * Anti-spoofing of these fields is out of scope for this PR (Stage-1 PR#6).
     */
    private fun relay(message: Message, hop: Int, originId: String?, exclude: Connection) {
        // Leaf fan-out: the original star behaviour, computed once and shared.
        val leafLine: String by lazy { MessageWire.encode(message) }
        for ((id, conn) in clients) {
            if (id == exclude.id) continue
            if (!conn.isBridge) {
                writeTo(conn, leafLine)
                continue
            }
            // Bridge forward: ask the policy whether (and at what hop) to cross.
            val nextHop = events.bridgeHopFor(message.id, hop) ?: continue
            // First crossing of a leaf-authored message records its origin; an
            // already-stamped origin is preserved across further hops.
            val stampedOrigin = originId ?: message.senderId
            writeTo(conn, MessageWire.encode(message, hop = nextHop, originId = stampedOrigin))
        }
    }

    private fun writeTo(conn: Connection, line: String): Boolean {
        return try {
            synchronized(conn.writeLock) {
                conn.out.write(line.toByteArray(Charsets.UTF_8))
                conn.out.flush()
            }
            true
        } catch (e: Exception) {
            Log.w(TAG, "Write to client ${conn.id} failed: ${e.message}")
            drop(conn)
            false
        }
    }

    private fun drop(conn: Connection) {
        if (clients.remove(conn.id) != null) {
            runCatching { conn.socket.close() }
            publishPeerCount()
            Log.i(TAG, "Client ${conn.id} disconnected (${clients.size} left)")
        }
    }

    private fun publishPeerCount() {
        _state.value = MeshState.Hub(clients.size, bridgePresence())
    }

    /** Re-publishes Hub state when a bridge connection flips isBridge. */
    private fun publishBridgePresence() = publishPeerCount()

    /**
     * Bridge liveness from THIS hub's accepted connections (the B-side view):
     *  - `null`  — no connection has ever announced itself as a bridge, so this
     *    hub is a plain star hub. Keeps [describe] byte-for-byte identical to the
     *    pre-bridge string (zero regression).
     *  - `true`  — at least one bridge connection is currently open.
     *  - `false` — a bridge was seen this session but its socket has since
     *    dropped (the [sawBridge] latch stays set so we report "down", not
     *    "absent", until the link reconnects).
     */
    private fun bridgePresence(): Boolean? {
        val live = clients.values.any { it.isBridge }
        if (live) {
            sawBridge = true
            return true
        }
        return if (sawBridge) false else null
    }

    // Latches once any bridge connection has been seen, so a dropped bridge
    // reports "down" (false) rather than reverting to "absent" (null).
    @Volatile
    private var sawBridge: Boolean = false

    /** Local fan-out: a message authored on the hub goes to all leaves. */
    override suspend fun send(message: Message): Boolean =
        sendRaw(MessageWire.encode(message))

    override suspend fun sendRaw(line: String): Boolean {
        var delivered = false
        for ((_, conn) in clients) {
            if (writeTo(conn, line)) delivered = true
        }
        // A hub with zero clients still "owns" the message locally; the caller
        // persisted it already. Report delivery only if a socket took it.
        return delivered
    }

    override suspend fun stop() {
        acceptJob?.cancel()
        acceptJob = null
        runCatching { serverSocket?.close() }
        serverSocket = null
        clients.values.forEach { runCatching { it.socket.close() } }
        clients.clear()
        scope.coroutineContext.cancel()
        _state.value = MeshState.Idle
        Log.i(TAG, "Stopped")
    }
}
