package ai.liquidway.lfmsmoke.data

import androidx.room.Entity
import androidx.room.PrimaryKey

/**
 * Lifecycle of a message as it moves through the (future) P2P sync layer.
 *
 *  - [LOCAL]  : created on this device, not yet pushed to a server.
 *  - [SENT]   : handed to the transport layer (delivery not yet confirmed).
 *  - [SYNCED] : acknowledged / persisted by the server.
 *
 * Networking is intentionally out of scope for this foundation layer; only
 * [LOCAL] is produced today, but the enum is fixed now so the schema is stable.
 */
enum class MessageStatus {
    LOCAL,
    SENT,
    SYNCED,
}

/**
 * A single chat message.
 *
 * [id] is a client-generated UUID and the primary key. Using a stable UUID
 * (rather than an auto-increment) lets the future sync layer deduplicate
 * messages received from multiple peers (insert is OnConflict.IGNORE).
 */
@Entity(tableName = "messages")
data class Message(
    @PrimaryKey
    val id: String,
    val senderId: String,
    val senderName: String,
    val body: String,
    /** Creation time in epoch milliseconds (also the sort key). */
    val createdAt: Long,
    val status: MessageStatus,
)
