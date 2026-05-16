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
                            // Relay to every *other* client so the star behaves
                            // as a bus. Dedup is the leaves' job (DAO
                            // OnConflict.IGNORE); relaying unconditionally keeps
                            // the hub stateless and simple.
                            relay(f.message, exclude = conn.id)
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
                        MessageWire.Frame.Unknown -> Unit // skip; keep the link
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Client ${conn.id} read error: ${e.message}")
            } finally {
                drop(conn)
            }
        }
    }

    private fun relay(message: Message, exclude: Long) {
        val line = MessageWire.encode(message)
        for ((id, conn) in clients) {
            if (id == exclude) continue
            writeTo(conn, line)
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
        _state.value = MeshState.Hub(clients.size)
    }

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
