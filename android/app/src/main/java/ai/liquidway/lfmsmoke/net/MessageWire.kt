package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.AI_SENDER_ID as DATA_AI_SENDER_ID
import ai.liquidway.lfmsmoke.data.AI_SENDER_NAME as DATA_AI_SENDER_NAME
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import org.json.JSONObject

/**
 * The on-the-wire framing for LiqMesh.
 *
 * One frame == one line of UTF-8 JSON terminated by '\n'. Line framing keeps
 * the reader trivial (BufferedReader.readLine) and is enough for short chat
 * payloads; a length-prefixed framing would be over-engineering at this stage.
 *
 * Layer 3 introduces a small envelope so the same socket carries three kinds of
 * frame:
 *
 *  - [TYPE_MSG]         : a chat [Message] (the layer-2 payload).
 *  - [TYPE_SYNC_REQ]    : a backfill request — "send me everything created
 *                         after `since` (epoch ms)". Sent by a leaf right after
 *                         it connects so it catches up on messages it missed
 *                         while offline.
 *  - [TYPE_SYNC_RESP]   : a single message replayed in answer to a sync request.
 *  - [TYPE_SUMMARY_REQ] : layer-4 — "run the on-device LFM over recent chat and
 *                         broadcast a situational summary". Any device can emit
 *                         it; only the hub (serverMode=ON) acts on it.
 *
 * Layer 4 deliberately does NOT add a new frame for the *result*: an AI summary
 * is materialised as an ordinary [TYPE_MSG] whose `senderId` is [AI_SENDER_ID],
 * so the existing relay, DAO dedup and backfill carry it for free and old peers
 * render it as a normal (visually-distinguished) bubble.
 *
 * BACKWARD COMPATIBILITY: a frame *without* a `type` field is interpreted as
 * [TYPE_MSG]. Layer-2 peers emit exactly that shape, so an old peer and a new
 * peer still interoperate for plain chat (the old peer simply ignores the
 * sync_req / sync_resp lines it cannot parse — [decodeFrame] returns
 * [Frame.Unknown] and the reader skips it).
 *
 * The wire form deliberately omits [Message.status]: status is a *local*
 * lifecycle concept (LOCAL/SENT/SYNCED) and must never be dictated by a peer.
 * A decoded message is always materialised as [MessageStatus.SENT] because, by
 * definition, it successfully traversed a socket to reach this device.
 */
object MessageWire {

    /** Default TCP port for the LiqMesh hub. */
    const val DEFAULT_PORT = 8765

    const val TYPE_MSG = "msg"
    const val TYPE_SYNC_REQ = "sync_req"
    const val TYPE_SYNC_RESP = "sync_resp"
    const val TYPE_SUMMARY_REQ = "summary_req"

    /**
     * Stage-1 bridge frames. They are wire-level type/encode/decode only: no
     * relay path consumes them yet (dead-data until the 2-hub bridge lands).
     *
     *  - [TYPE_BRIDGE_HELLO]   : a hub announces itself across a bridge link
     *                            (future hub identification / liveness).
     *  - [TYPE_SUMMARY_CLAIM]  : ownership assertion so exactly one hub answers a
     *                            given summary question ("first to receive wins").
     */
    const val TYPE_BRIDGE_HELLO = "bridge_hello"
    const val TYPE_SUMMARY_CLAIM = "summary_claim"

    /**
     * Reserved sender id for AI-authored summary messages (re-exported from the
     * data package so wire-layer callers need not reach across packages). A
     * message with this sender renders as a system/AI bubble and is excluded
     * from the window fed back into the model.
     */
    const val AI_SENDER_ID = DATA_AI_SENDER_ID

    /** Display name carried on AI summary messages. */
    const val AI_SENDER_NAME = DATA_AI_SENDER_NAME

    /** A decoded inbound frame. */
    sealed interface Frame {
        /**
         * A chat message (plain layer-2 payload or a sync_resp replay).
         *
         * [hop] and [originId] are Stage-1 bridge envelope fields: [hop] is the
         * number of bridge crossings (0 at the originating device) and
         * [originId] is the deviceId that first injected the message. They are
         * dead-data today — no relay path reads them — and default so layer-2/3
         * call sites (and legacy peers) keep working unchanged.
         */
        data class Msg(
            val message: Message,
            val hop: Int = 0,
            val originId: String? = null,
        ) : Frame

        /** A backfill request: replay everything created after [since] (epoch ms). */
        data class SyncReq(val since: Long) : Frame

        /**
         * Layer-4 request to summarise recent chat. [since] is advisory only
         * (kept 0 today); the hub caps the window by count itself. Modelled as
         * a frame, not a [Message], because it triggers work rather than
         * carrying content.
         *
         * [questionId] is a Stage-1 bridge field (null today) that will let a
         * [SummaryClaim] reference the specific request being answered.
         */
        data class SummaryReq(val since: Long, val questionId: String? = null) : Frame

        /**
         * Stage-1 bridge: a hub's self-announcement across a bridge link, also
         * serving as the inter-hub liveness heartbeat.
         *
         * [queueDepth] and [dispatchCount] are the operator-layer load metrics
         * piggybacked on the existing heartbeat (no new frame, no new timer):
         *  - [queueDepth]    : how busy this hub's summariser is right now
         *                      (in-flight summaries; 0 when idle).
         *  - [dispatchCount] : how many summaries this hub has produced in total
         *                      (a monotonic counter for future load-based hints).
         *
         * Both are optional and default to 0, so a legacy hello (or a pre-metrics
         * peer) that omits them decodes cleanly as "idle / unknown load".
         *
         * [dispatchTarget] is the operator-layer-3 advisory dispatch hint: the
         * deviceId of the hub the SENDING hub (when it is the elected operator)
         * believes should preferentially claim a new summary — i.e. the least
         * loaded hub. It is OPTIONAL and null when the sender is not the operator
         * or has no opinion, so a legacy / non-operator hello omits it and decodes
         * as null. It is ONLY a hint: it biases *when* a hub tries to claim, never
         * *whether* a question ends up single-owned — the claim CAS
         * ([QuestionOwnership]) remains the correctness floor regardless of the
         * hint's value.
         */
        data class BridgeHello(
            val deviceId: String,
            val queueDepth: Int = 0,
            val dispatchCount: Int = 0,
            val dispatchTarget: String? = null,
        ) : Frame

        /**
         * Stage-1 bridge: ownership claim for answering [questionId], asserted by
         * [ownerId]. Decode/encode only — no relay path consumes it yet.
         */
        data class SummaryClaim(val questionId: String, val ownerId: String) : Frame

        /** Malformed or unrecognised line; the reader skips it without dropping the link. */
        data object Unknown : Frame
    }

    /** Builds an AI-authored summary [Message] (rides the normal msg path). */
    fun aiSummaryMessage(id: String, body: String, createdAt: Long): Message =
        Message(
            id = id,
            senderId = AI_SENDER_ID,
            senderName = AI_SENDER_NAME,
            body = body,
            createdAt = createdAt,
            status = MessageStatus.SENT,
        )

    /**
     * Serialises a chat message as a [TYPE_MSG] frame.
     *
     * [hop]/[originId] are the Stage-1 bridge envelope fields; they default so
     * existing layer-2/3 callers (which pass only [message]) are unaffected and
     * emit the same shape as before (hop=0, originId omitted).
     */
    fun encode(message: Message, hop: Int = 0, originId: String? = null): String =
        encodeMessage(message, TYPE_MSG, hop, originId)

    /** Serialises a chat message as a [TYPE_SYNC_RESP] backfill frame. */
    fun encodeSyncResp(message: Message): String = encodeMessage(message, TYPE_SYNC_RESP)

    /** Serialises a backfill request carrying the requester's high-water mark. */
    fun encodeSyncReq(since: Long): String =
        JSONObject().put("type", TYPE_SYNC_REQ).put("since", since).toString() + "\n"

    /**
     * Serialises a layer-4 "summarise recent chat" request. [questionId] is the
     * Stage-1 bridge field (null = omitted) for later claim correlation.
     */
    fun encodeSummaryReq(since: Long = 0L, questionId: String? = null): String {
        val json = JSONObject().put("type", TYPE_SUMMARY_REQ).put("since", since)
        if (questionId != null) json.put("questionId", questionId)
        return json.toString() + "\n"
    }

    /**
     * Serialises a Stage-1 [TYPE_BRIDGE_HELLO] hub self-announcement, with the
     * operator-layer load metrics [queueDepth]/[dispatchCount] piggybacked. Both
     * default to 0 so an idle hub (and existing call sites) emit the same shape
     * as before plus two zero fields a legacy peer simply ignores.
     */
    fun encodeBridgeHello(
        deviceId: String,
        queueDepth: Int = 0,
        dispatchCount: Int = 0,
        dispatchTarget: String? = null,
    ): String {
        val json = JSONObject()
            .put("type", TYPE_BRIDGE_HELLO)
            .put("deviceId", deviceId)
            .put("queueDepth", queueDepth)
            .put("dispatchCount", dispatchCount)
        // Omitted entirely when the sender has no hint (not the operator), so a
        // legacy peer and the absence of a hint decode identically as null.
        if (dispatchTarget != null) json.put("dispatchTarget", dispatchTarget)
        return json.toString() + "\n"
    }

    /** Serialises a Stage-1 [TYPE_SUMMARY_CLAIM] ownership assertion. */
    fun encodeSummaryClaim(questionId: String, ownerId: String): String =
        JSONObject()
            .put("type", TYPE_SUMMARY_CLAIM)
            .put("questionId", questionId)
            .put("ownerId", ownerId)
            .toString() + "\n"

    private fun encodeMessage(
        message: Message,
        type: String,
        hop: Int = 0,
        originId: String? = null,
    ): String {
        val json = JSONObject()
            .put("type", type)
            .put("id", message.id)
            .put("senderId", message.senderId)
            .put("senderName", message.senderName)
            .put("body", message.body)
            .put("createdAt", message.createdAt)
            .put("hop", hop)
        if (originId != null) json.put("originId", originId)
        return json.toString() + "\n"
    }

    /**
     * Parses one JSON line into a [Frame].
     *
     * Returns [Frame.Unknown] for malformed input (missing field / bad JSON /
     * unrecognised type) so a single corrupt or future-typed line from a peer
     * can be skipped without tearing down the connection.
     */
    fun decodeFrame(line: String): Frame {
        val trimmed = line.trim()
        if (trimmed.isEmpty()) return Frame.Unknown
        return try {
            val json = JSONObject(trimmed)
            // Absent type == legacy layer-2 chat message (backward compat).
            when (json.optString("type", TYPE_MSG)) {
                TYPE_MSG, TYPE_SYNC_RESP -> Frame.Msg(
                    Message(
                        id = json.getString("id"),
                        senderId = json.getString("senderId"),
                        senderName = json.getString("senderName"),
                        body = json.getString("body"),
                        createdAt = json.getLong("createdAt"),
                        status = MessageStatus.SENT,
                    ),
                    // Bridge envelope: absent on legacy peers -> hop 0 / no origin.
                    hop = json.optInt("hop", 0),
                    originId = json.optStringOrNull("originId"),
                )
                TYPE_SYNC_REQ -> Frame.SyncReq(json.getLong("since"))
                TYPE_SUMMARY_REQ -> Frame.SummaryReq(
                    json.optLong("since", 0L),
                    json.optStringOrNull("questionId"),
                )
                TYPE_BRIDGE_HELLO -> Frame.BridgeHello(
                    json.getString("deviceId"),
                    // Absent on a legacy / pre-metrics hello -> idle (0) load.
                    queueDepth = json.optInt("queueDepth", 0),
                    dispatchCount = json.optInt("dispatchCount", 0),
                    // Absent on a legacy / non-operator hello -> no hint (null).
                    dispatchTarget = json.optStringOrNull("dispatchTarget"),
                )
                TYPE_SUMMARY_CLAIM -> Frame.SummaryClaim(
                    json.getString("questionId"),
                    json.getString("ownerId"),
                )
                else -> Frame.Unknown
            }
        } catch (_: Exception) {
            Frame.Unknown
        }
    }

    /**
     * Legacy helper kept for the layer-2 tests: decodes a line to a [Message]
     * or null. Equivalent to taking the [Frame.Msg] case of [decodeFrame].
     */
    fun decode(line: String): Message? =
        (decodeFrame(line) as? Frame.Msg)?.message

    /**
     * Returns the string at [key], or null when the key is absent or JSON-null.
     * org.json's `optString` collapses both to "", which we must not treat as a
     * present empty value for optional bridge fields.
     */
    private fun JSONObject.optStringOrNull(key: String): String? =
        if (isNull(key) || !has(key)) null else getString(key)
}
