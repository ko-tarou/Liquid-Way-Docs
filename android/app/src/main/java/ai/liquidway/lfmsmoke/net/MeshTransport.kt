package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message
import kotlinx.coroutines.flow.StateFlow

/**
 * A running LiqMesh layer-2 transport (either the hub or a single leaf).
 *
 * Lifecycle: construct -> [start] -> [send] zero or more times -> [stop].
 * Implementations are responsible for all socket I/O on [kotlinx.coroutines.Dispatchers.IO]
 * and must be leak-free once [stop] returns.
 *
 * Inbound messages are NOT pushed to a callback here; instead the owner passes
 * an `onInbound` suspend lambda at construction so the persistence + relay
 * policy lives in one place ([MeshController]). This keeps the transport a dumb
 * pipe and leaves room for the layer-3 outbox to wrap [send] later.
 */
interface MeshTransport {

    /** Observable connection state for the UI. */
    val state: StateFlow<MeshState>

    /** Begins listening / connecting. Suspends only to spin up; returns fast. */
    suspend fun start()

    /**
     * Best-effort delivery of a locally-authored message.
     *
     * Hub: fan-out to every connected client.
     * Leaf: write to the server socket.
     *
     * @return true if the bytes were handed to at least one live socket. A
     *   false return means "stayed LOCAL" (layer 3 will retry); we never fake
     *   a SENT transition on a dead link.
     */
    suspend fun send(message: Message): Boolean

    /** Tears down all sockets and coroutines. Idempotent. */
    suspend fun stop()
}
