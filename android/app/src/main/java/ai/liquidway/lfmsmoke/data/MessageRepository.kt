package ai.liquidway.lfmsmoke.data

import ai.liquidway.lfmsmoke.settings.SettingsRepository
import android.content.Context
import kotlinx.coroutines.flow.Flow
import java.util.UUID

/**
 * Single source of truth for chat messages.
 *
 * This foundation layer only produces [MessageStatus.LOCAL] messages (no
 * networking yet). The future P2P layer will reuse [MessageDao.insert]'s
 * OnConflict.IGNORE for cross-peer dedup and flip statuses via
 * [MessageDao.updateStatus].
 */
class MessageRepository(
    private val dao: MessageDao,
    private val settings: SettingsRepository,
) {

    val messages: Flow<List<Message>> = dao.observeAll()

    /** Live count of un-sent (LOCAL) messages, surfaced subtly in the UI. */
    val pendingCount: Flow<Int> = dao.observePendingCount()

    /**
     * The single outbound window. The networking layer ([MeshController])
     * installs itself here; until then sends are a no-op (message stays
     * LOCAL). Layer 3's outbox will wrap this same hook — the UI never learns
     * about the transport.
     */
    @Volatile
    var transportSender: (suspend (Message) -> Unit)? = null

    /**
     * Creates a message authored by this device, persists it as [LOCAL],
     * hands it to the transport (if any) and returns it. Blank bodies are
     * rejected (returns null).
     */
    suspend fun addLocal(body: String): Message? {
        val text = body.trim()
        if (text.isEmpty()) return null

        val message = Message(
            id = UUID.randomUUID().toString(),
            senderId = settings.deviceId(),
            senderName = settings.currentDeviceName(),
            body = text,
            createdAt = System.currentTimeMillis(),
            status = MessageStatus.LOCAL,
        )
        dao.insert(message)
        transportSender?.invoke(message)
        return message
    }

    /**
     * Persists a message received from a peer. Dedup is handled by the DAO
     * (OnConflict.IGNORE on the UUID primary key).
     *
     * @return true if the row was newly inserted, false if it was a duplicate.
     */
    suspend fun acceptRemote(message: Message): Boolean {
        return dao.insertReturning(message) != -1L
    }

    /** Transport-driven status transition (e.g. LOCAL -> SENT on handoff). */
    suspend fun updateStatus(id: String, status: MessageStatus) {
        dao.updateStatus(id, status)
    }

    /**
     * Snapshot of the send outbox: locally-authored messages still LOCAL
     * (never handed to a socket), oldest first. Layer 3 drains this on
     * (re)connect.
     */
    suspend fun outbox(): List<Message> = dao.pendingOutbox()

    /**
     * Messages this device holds that were created after [since], capped at
     * [limit] and returned oldest-first so the peer can insert them in chat
     * order. Used to answer a backfill (sync_req) from a peer.
     */
    suspend fun backfillSince(since: Long, limit: Int): List<Message> =
        dao.recentSince(since, limit).asReversed()

    /**
     * Backfill watermark: highest createdAt among messages received from a
     * peer (status != LOCAL), or 0 when none. Excludes this device's own
     * offline-queued LOCAL rows so they cannot mask peer messages still needed.
     */
    suspend fun latestCreatedAt(): Long = dao.maxSyncedCreatedAt() ?: 0L

    companion object {
        @Volatile
        private var instance: MessageRepository? = null

        fun get(context: Context): MessageRepository {
            return instance ?: synchronized(this) {
                instance ?: MessageRepository(
                    dao = AppDatabase.get(context).messageDao(),
                    settings = SettingsRepository.get(context),
                ).also { instance = it }
            }
        }
    }
}
