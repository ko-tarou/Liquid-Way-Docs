package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message
import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket

/**
 * A single outbound socket to a hub, used in two roles:
 *
 *  - [Role.PRIMARY] — a leaf (serverMode = OFF) reaching its hub. Inbound lines
 *    are decoded and persisted via [MeshEvents]; there is no relay (the hub owns
 *    fan-out). This is the unchanged layer-2/3 behaviour.
 *  - [Role.BRIDGE] — Stage-1 bridge: a *hub* reaching a *second* hub so the two
 *    star networks merge. On connect it announces itself with a
 *    [MessageWire.Frame.BridgeHello] so the far hub marks this link `isBridge`
 *    and forwards across it (PR#3). Inbound chat from the far hub is funnelled
 *    into [MeshEvents.ingest] with `fromBridge=true` so the owning controller
 *    can persist it and fan it out to THIS hub's local leaves.
 *
 * A supervised loop keeps reconnecting with a capped backoff so a hub restart
 * or transient Wi-Fi blip self-heals.
 *
 * Layer 3: each time the socket comes up, [MeshEvents.onLinkEstablished] is
 * invoked so the controller can flush the outbox and request backfill. Messages
 * that fail to send while disconnected stay LOCAL — [send] never lies about
 * delivery; the outbox flush replays them on the next link.
 */
class MeshClient(
    private val host: String,
    private val port: Int = MessageWire.DEFAULT_PORT,
    private val events: MeshEvents,
    private val role: Role = Role.PRIMARY,
    /**
     * BRIDGE role only: this hub's own deviceId, sent in the opening
     * [MessageWire.Frame.BridgeHello]. Null for a PRIMARY leaf (no hello).
     */
    private val deviceId: String? = null,
) : MeshTransport {

    /** The two ways a single outbound socket is used. See the class doc. */
    enum class Role { PRIMARY, BRIDGE }

    private companion object {
        const val TAG = "LiqMesh/Client"
        // Start retries quickly: a hub may come up moments after the leaf, and
        // some networks RST the first connect right after the hub binds. Fast
        // initial retries make join latency low; backoff still caps churn.
        const val MIN_BACKOFF_MS = 250L
        const val MAX_BACKOFF_MS = 15_000L
        const val CONNECT_TIMEOUT_MS = 5_000

        /**
         * Bridge liveness: a BRIDGE-role client re-sends its [bridge_hello] on
         * this cadence so the far hub can show "bridge alive" (last-seen). It
         * deliberately re-uses the existing hello frame — no new wire type, so
         * old peers stay compatible. Death detection is NOT timer-driven here:
         * the TCP read EOF + capped-backoff reconnect already self-heal a dropped
         * link, so an active timeout-disconnect would be redundant complexity.
         */
        const val BRIDGE_HEARTBEAT_MS = 5_000L
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loopJob: Job? = null

    // BRIDGE role only: re-sends bridge_hello on a cadence. Restarted on every
    // (re)connect (so it writes to the live socket) and cancelled on link loss.
    private var heartbeatJob: Job? = null

    @Volatile
    private var socket: Socket? = null

    @Volatile
    private var out: OutputStream? = null
    private val writeLock = Any()

    private val _state = MutableStateFlow<MeshState>(MeshState.Idle)
    override val state: StateFlow<MeshState> = _state.asStateFlow()

    override suspend fun start() {
        if (loopJob != null) return
        loopJob = scope.launch { connectLoop() }
    }

    private suspend fun connectLoop() {
        var backoff = MIN_BACKOFF_MS
        while (scope.isActive) {
            _state.value = MeshState.Connecting(host, port)
            val s = try {
                withContext(Dispatchers.IO) {
                    // Pre-resolve to a concrete InetAddress and connect via an
                    // explicitly *unbound* socket. Passing a hostname string to
                    // connect() can make the JVM attempt an implicit local bind
                    // that fails with BindException ("Can't assign requested
                    // address") under rapid reconnects; a resolved address
                    // avoids that. tcpNoDelay is set only after connect — doing
                    // it earlier force-creates the socket impl and reintroduces
                    // the same BindException.
                    val target = InetSocketAddress(
                        java.net.InetAddress.getByName(host),
                        port,
                    )
                    Socket().apply {
                        connect(target, CONNECT_TIMEOUT_MS)
                        tcpNoDelay = true
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Connect to $host:$port failed: ${e.message}")
                _state.value = MeshState.Disconnected(e.message ?: "connect failed")
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
                continue
            }

            socket = s
            out = s.getOutputStream()
            backoff = MIN_BACKOFF_MS
            _state.value = MeshState.Connected(host, port)
            Log.i(TAG, "Connected to $host:$port (role=$role)")

            // BRIDGE role: announce ourselves so the far hub flips this link to
            // isBridge=true and forwards chat across it (the B->A direction).
            // Sent inline before onLinkEstablished so the hello is the first
            // line the far hub reads on this connection.
            if (role == Role.BRIDGE) {
                sendRaw(MessageWire.encodeBridgeHello(deviceId ?: "bridge"))
                startHeartbeat()
            }

            // Layer 3: drain the outbox and ask the hub for anything we missed
            // while offline. Done off the reader path so a slow flush cannot
            // delay reading inbound backfill.
            scope.launch { events.onLinkEstablished(this@MeshClient) }

            try {
                val reader = BufferedReader(
                    InputStreamReader(s.getInputStream(), Charsets.UTF_8),
                )
                while (scope.isActive) {
                    val line = reader.readLine() ?: break
                    when (val f = MessageWire.decodeFrame(line)) {
                        is MessageWire.Frame.Msg ->
                            // PRIMARY leaf: just persist (the hub owns fan-out).
                            // BRIDGE: funnel far-hub chat into the unified ingest
                            // so the controller persists it AND fans it out to
                            // this hub's local leaves. fromBridge marks the inbound
                            // edge so it is never echoed back across this bridge.
                            events.ingest(
                                f.message,
                                hop = f.hop,
                                originId = f.originId,
                                fromBridge = role == Role.BRIDGE,
                            )
                        is MessageWire.Frame.SyncReq ->
                            if (role == Role.BRIDGE) {
                                // Partition recovery (A->B direction): the far
                                // hub (B) asked THIS hub (A) for backfill on the
                                // bridge link. Answer from our store onto the
                                // same socket, exactly as a hub answers a leaf.
                                // A PRIMARY leaf still ignores sync_req (below).
                                events.onSyncRequest(f.since) { reply -> sendRaw(reply) }
                            } else {
                                // A leaf never serves backfill (only the hub
                                // does); ignore defensively.
                                Log.d(TAG, "Ignoring sync_req on leaf")
                            }
                        is MessageWire.Frame.SummaryReq ->
                            // A leaf never runs the model; the hub does. The
                            // hub does not relay summary_req, so a leaf should
                            // not see this — ignore defensively.
                            Log.d(TAG, "Ignoring summary_req on leaf")
                        // Stage-1 bridge frames + Unknown: no relay path consumes
                        // them yet, so skip without dropping the link.
                        is MessageWire.Frame.BridgeHello,
                        is MessageWire.Frame.SummaryClaim,
                        MessageWire.Frame.Unknown,
                        -> Unit
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Read loop ended: ${e.message}")
            } finally {
                heartbeatJob?.cancel()
                heartbeatJob = null
                cleanupSocket()
            }

            if (scope.isActive) {
                _state.value = MeshState.Disconnected("link lost")
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
            }
        }
    }

    /**
     * BRIDGE liveness: re-send [bridge_hello] every [BRIDGE_HEARTBEAT_MS] on the
     * live socket. Launched after the opening hello on each (re)connect and
     * cancelled when the link drops, so it never writes to a stale socket. A
     * failed write returns false and tears the socket down via [sendRaw], which
     * the connect loop's backoff reconnect then heals — we do not loop on error.
     */
    private fun startHeartbeat() {
        heartbeatJob?.cancel()
        heartbeatJob = scope.launch {
            while (isActive) {
                delay(BRIDGE_HEARTBEAT_MS)
                if (!sendRaw(MessageWire.encodeBridgeHello(deviceId ?: "bridge"))) break
            }
        }
    }

    override suspend fun send(message: Message): Boolean =
        sendRaw(MessageWire.encode(message))

    override suspend fun sendRaw(line: String): Boolean {
        val stream = out ?: return false
        return try {
            val bytes = line.toByteArray(Charsets.UTF_8)
            synchronized(writeLock) {
                stream.write(bytes)
                stream.flush()
            }
            true
        } catch (e: Exception) {
            Log.w(TAG, "Send failed: ${e.message}")
            cleanupSocket()
            false
        }
    }

    private fun cleanupSocket() {
        out = null
        socket?.let { runCatching { it.close() } }
        socket = null
    }

    override suspend fun stop() {
        heartbeatJob?.cancel()
        heartbeatJob = null
        loopJob?.cancel()
        loopJob = null
        cleanupSocket()
        scope.coroutineContext.cancel()
        _state.value = MeshState.Idle
        Log.i(TAG, "Stopped")
    }
}
