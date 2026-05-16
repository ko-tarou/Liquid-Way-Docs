package ai.liquidway.lfmsmoke.data

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow

@Dao
interface MessageDao {

    /**
     * Inserts a message, ignoring it if a row with the same [Message.id]
     * already exists. This is the dedup primitive for the future sync layer:
     * the same message arriving from multiple peers collapses to one row.
     */
    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insert(message: Message)

    /** Observes all messages, oldest first (chat order). */
    @Query("SELECT * FROM messages ORDER BY createdAt ASC")
    fun observeAll(): Flow<List<Message>>

    @Query("UPDATE messages SET status = :status WHERE id = :id")
    suspend fun updateStatus(id: String, status: MessageStatus)
}
