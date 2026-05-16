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

    /**
     * Creates a message authored by this device, persists it as [LOCAL] and
     * returns it. Blank bodies are rejected (returns null).
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
        return message
    }

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
