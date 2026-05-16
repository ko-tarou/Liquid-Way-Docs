package ai.liquidway.lfmsmoke.net

/**
 * Observable connection state for the LiqMesh layer-2 transport.
 *
 * This is the single type the UI binds to, regardless of whether the device is
 * currently the hub (server) or a leaf (client). Keeping one sealed model
 * avoids the screen having to branch on mode.
 */
sealed interface MeshState {

    /** Transport not running (server mode toggled but service not started, etc). */
    data object Idle : MeshState

    /** Acting as the star hub; [peerCount] clients are currently connected. */
    data class Hub(val peerCount: Int) : MeshState

    /** Acting as a leaf node, trying to reach [host]:[port]. */
    data class Connecting(val host: String, val port: Int) : MeshState

    /** Acting as a leaf node and connected to [host]:[port]. */
    data class Connected(val host: String, val port: Int) : MeshState

    /** Acting as a leaf node, disconnected; [reason] is a short human note. */
    data class Disconnected(val reason: String) : MeshState
}
