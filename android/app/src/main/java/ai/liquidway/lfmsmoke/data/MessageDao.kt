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

    /**
     * Same as [insert] but returns the inserted rowId, or -1 when the row was
     * ignored as a duplicate. This is the dedup signal the relay layer uses to
     * distinguish "new message" from "already seen".
     */
    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertReturning(message: Message): Long

    /** Observes all messages, oldest first (chat order). */
    @Query("SELECT * FROM messages ORDER BY createdAt ASC")
    fun observeAll(): Flow<List<Message>>

    /** One-shot snapshot of all messages (used by deterministic tests). */
    @Query("SELECT * FROM messages ORDER BY createdAt ASC")
    suspend fun observeAllNow(): List<Message>

    /**
     * The send outbox: messages authored here that never made it onto a socket
     * (status still LOCAL), oldest first so a flush preserves authoring order.
     */
    @Query("SELECT * FROM messages WHERE status = 'LOCAL' ORDER BY createdAt ASC")
    suspend fun pendingOutbox(): List<Message>

    /** Live count of un-sent (LOCAL) messages, for the subtle UI indicator. */
    @Query("SELECT COUNT(*) FROM messages WHERE status = 'LOCAL'")
    fun observePendingCount(): Flow<Int>

    /**
     * Backfill source: the most recent messages created strictly after [since],
     * newest-first then [limit]-capped. The caller re-sorts ascending before
     * replay so the receiver inserts them in chat order.
     */
    @Query(
        "SELECT * FROM messages WHERE createdAt > :since " +
            "ORDER BY createdAt DESC LIMIT :limit",
    )
    suspend fun recentSince(since: Long, limit: Int): List<Message>

    /**
     * Layer-4 summary window: the most recent human chat messages, excluding
     * AI-authored summaries (so a summary never feeds back into itself),
     * newest-first then [limit]-capped. The caller re-sorts ascending so the
     * model reads the conversation in chat order.
     */
    @Query(
        "SELECT * FROM messages WHERE senderId != :aiSenderId " +
            "ORDER BY createdAt DESC LIMIT :limit",
    )
    suspend fun recentForSummary(aiSenderId: String, limit: Int): List<Message>

    /**
     * Largest createdAt among messages this device did *not* author locally
     * (status != LOCAL), or null when none. This is the backfill watermark:
     * using only delivered/received rows means a leaf's own offline-queued
     * LOCAL messages (which can have a high createdAt) never advance the mark
     * and therefore never mask a peer message it still needs to catch up on.
     */
    @Query("SELECT MAX(createdAt) FROM messages WHERE status != 'LOCAL'")
    suspend fun maxSyncedCreatedAt(): Long?

    /**
     * Status flip. Guarded so a transition can only move *forward*
     * (LOCAL -> SENT -> SYNCED): a late/duplicate update can never demote a row
     * (e.g. SENT back to LOCAL). Ordinal order encodes the lifecycle.
     */
    @Query(
        "UPDATE messages SET status = :status WHERE id = :id " +
            "AND (CASE status " +
            "WHEN 'LOCAL' THEN 0 WHEN 'SENT' THEN 1 WHEN 'SYNCED' THEN 2 END) " +
            "< (CASE :status " +
            "WHEN 'LOCAL' THEN 0 WHEN 'SENT' THEN 1 WHEN 'SYNCED' THEN 2 END)",
    )
    suspend fun updateStatus(id: String, status: MessageStatus)
}
