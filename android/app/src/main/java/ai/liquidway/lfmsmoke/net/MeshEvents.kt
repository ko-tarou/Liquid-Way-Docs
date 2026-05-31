package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message

/**
 * The policy hooks a layer-2 transport calls into. All persistence, dedup,
 * outbox-flush and backfill logic lives behind this interface (implemented by
 * [MeshController]) so the transports stay dumb pipes.
 *
 * Every method is a suspend function invoked on Dispatchers.IO from the
 * transport's reader/connect coroutines; implementations must be cheap or
 * dispatch their own work.
 */
interface MeshEvents {

    /**
     * Persist an inbound chat message (plain msg or sync_resp replay).
     * @return true if it was newly stored (false == duplicate; dedup is the DAO's).
     */
    suspend fun onMessage(message: Message): Boolean

    /**
     * A peer asked for backfill: everything we hold created after [since].
     * [reply] frames each missing message back to *that one* peer.
     */
    suspend fun onSyncRequest(since: Long, reply: suspend (line: String) -> Unit)

    /**
     * A usable link just came up.
     *
     * Leaf: its single socket connected — flush the outbox and ask the hub for
     * backfill. Hub: a new client attached — used only for logging; the hub
     * answers backfill reactively via [onSyncRequest].
     */
    suspend fun onLinkEstablished(transport: MeshTransport)

    /**
     * Layer 4: a device asked for a situational summary of recent chat.
     *
     * Only the hub (serverMode=ON) acts on this — it runs the on-device LFM
     * over the recent window and broadcasts the result as an ordinary AI
     * message. A leaf receiving this (it never should, the hub does fan-out
     * for chat not for summary_req) ignores it. The implementation must NOT
     * block the caller (transport reader): heavyweight generation is dispatched
     * onto a separate low-priority coroutine so plain chat keeps relaying.
     *
     * @param since advisory high-water mark (0 = "recent window, hub decides").
     */
    suspend fun onSummaryRequest(since: Long)

    /**
     * Stage-1 bridge: decide whether a message may be forwarded across an
     * inter-hub bridge, and at what hop. The hub's reader calls this before
     * relaying a chat message onto a *bridge* connection (never onto a plain
     * leaf — leaf fan-out stays policy-free and unchanged).
     *
     * Delegates to [MeshController]'s loop-prevention policy (seen-set + hop
     * ceiling). Pure, non-suspending decision so a reader can call it inline.
     *
     * @return the hop to stamp on the forwarded frame ([hop] + 1), or null to
     *   NOT forward (already-seen loop break, or hop ceiling reached).
     */
    fun bridgeHopFor(messageId: String, hop: Int): Int?
}
