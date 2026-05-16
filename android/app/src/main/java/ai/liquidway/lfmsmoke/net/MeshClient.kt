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
 * A leaf node (serverMode = OFF) that maintains a single socket to the hub.
 *
 * A supervised loop keeps reconnecting with a capped backoff so a hub restart
 * or transient Wi-Fi blip self-heals. Inbound lines are persisted via
 * [onInbound]; there is no relay (the hub owns fan-out).
 *
 * NOTE: messages that fail to send while disconnected are intentionally left
 * as LOCAL — replaying them is layer 3 (outbox). [send] never lies about
 * delivery.
 */
class MeshClient(
    private val host: String,
    private val port: Int = MessageWire.DEFAULT_PORT,
    private val onInbound: suspend (Message) -> Boolean,
) : MeshTransport {

    private companion object {
        const val TAG = "LiqMesh/Client"
        // Start retries quickly: a hub may come up moments after the leaf, and
        // some networks RST the first connect right after the hub binds. Fast
        // initial retries make join latency low; backoff still caps churn.
        const val MIN_BACKOFF_MS = 250L
        const val MAX_BACKOFF_MS = 15_000L
        const val CONNECT_TIMEOUT_MS = 5_000
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var loopJob: Job? = null

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
            Log.i(TAG, "Connected to $host:$port")

            try {
                val reader = BufferedReader(
                    InputStreamReader(s.getInputStream(), Charsets.UTF_8),
                )
                while (scope.isActive) {
                    val line = reader.readLine() ?: break
                    MessageWire.decode(line)?.let { onInbound(it) }
                }
            } catch (e: Exception) {
                Log.w(TAG, "Read loop ended: ${e.message}")
            } finally {
                cleanupSocket()
            }

            if (scope.isActive) {
                _state.value = MeshState.Disconnected("link lost")
                delay(backoff)
                backoff = (backoff * 2).coerceAtMost(MAX_BACKOFF_MS)
            }
        }
    }

    override suspend fun send(message: Message): Boolean {
        val stream = out ?: return false
        return try {
            val bytes = MessageWire.encode(message).toByteArray(Charsets.UTF_8)
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
        loopJob?.cancel()
        loopJob = null
        cleanupSocket()
        scope.coroutineContext.cancel()
        _state.value = MeshState.Idle
        Log.i(TAG, "Stopped")
    }
}
