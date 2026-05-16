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
        /** A chat message (plain layer-2 payload or a sync_resp replay). */
        data class Msg(val message: Message) : Frame

        /** A backfill request: replay everything created after [since] (epoch ms). */
        data class SyncReq(val since: Long) : Frame

        /**
         * Layer-4 request to summarise recent chat. [since] is advisory only
         * (kept 0 today); the hub caps the window by count itself. Modelled as
         * a frame, not a [Message], because it triggers work rather than
         * carrying content.
         */
        data class SummaryReq(val since: Long) : Frame

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

    /** Serialises a chat message as a [TYPE_MSG] frame. */
    fun encode(message: Message): String = encodeMessage(message, TYPE_MSG)

    /** Serialises a chat message as a [TYPE_SYNC_RESP] backfill frame. */
    fun encodeSyncResp(message: Message): String = encodeMessage(message, TYPE_SYNC_RESP)

    /** Serialises a backfill request carrying the requester's high-water mark. */
    fun encodeSyncReq(since: Long): String =
        JSONObject().put("type", TYPE_SYNC_REQ).put("since", since).toString() + "\n"

    /** Serialises a layer-4 "summarise recent chat" request. */
    fun encodeSummaryReq(since: Long = 0L): String =
        JSONObject().put("type", TYPE_SUMMARY_REQ).put("since", since).toString() + "\n"

    private fun encodeMessage(message: Message, type: String): String {
        val json = JSONObject()
            .put("type", type)
            .put("id", message.id)
            .put("senderId", message.senderId)
            .put("senderName", message.senderName)
            .put("body", message.body)
            .put("createdAt", message.createdAt)
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
                )
                TYPE_SYNC_REQ -> Frame.SyncReq(json.getLong("since"))
                TYPE_SUMMARY_REQ -> Frame.SummaryReq(json.optLong("since", 0L))
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
}
