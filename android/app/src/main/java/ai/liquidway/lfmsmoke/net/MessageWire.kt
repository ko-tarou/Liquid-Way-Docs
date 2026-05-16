package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import org.json.JSONObject

/**
 * The on-the-wire framing for LiqMesh layer 2 (same-network TCP star).
 *
 * One message == one line of UTF-8 JSON terminated by '\n'. Line framing keeps
 * the reader trivial (BufferedReader.readLine) and is enough for short chat
 * payloads; a length-prefixed framing would be over-engineering at this stage.
 *
 * The wire form deliberately omits [Message.status]: status is a *local*
 * lifecycle concept (LOCAL/SENT/SYNCED) and must never be dictated by a peer.
 * A decoded message is always materialised as [MessageStatus.SENT] because, by
 * definition, it successfully traversed a socket to reach this device.
 */
object MessageWire {

    /** Default TCP port for the LiqMesh hub. */
    const val DEFAULT_PORT = 8765

    /** Serialises a message to a single newline-terminated JSON line. */
    fun encode(message: Message): String {
        val json = JSONObject()
            .put("id", message.id)
            .put("senderId", message.senderId)
            .put("senderName", message.senderName)
            .put("body", message.body)
            .put("createdAt", message.createdAt)
        return json.toString() + "\n"
    }

    /**
     * Parses one JSON line into a [Message] tagged [MessageStatus.SENT].
     *
     * Returns null for malformed input (missing field / bad JSON) so a single
     * corrupt line from a peer can be skipped without tearing down the
     * connection.
     */
    fun decode(line: String): Message? {
        val trimmed = line.trim()
        if (trimmed.isEmpty()) return null
        return try {
            val json = JSONObject(trimmed)
            Message(
                id = json.getString("id"),
                senderId = json.getString("senderId"),
                senderName = json.getString("senderName"),
                body = json.getString("body"),
                createdAt = json.getLong("createdAt"),
                status = MessageStatus.SENT,
            )
        } catch (_: Exception) {
            null
        }
    }
}
