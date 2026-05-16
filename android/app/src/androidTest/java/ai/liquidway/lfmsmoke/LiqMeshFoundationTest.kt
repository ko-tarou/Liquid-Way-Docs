package ai.liquidway.lfmsmoke

import ai.liquidway.lfmsmoke.data.AppDatabase
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageDao
import ai.liquidway.lfmsmoke.data.MessageStatus
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * On-device verification of the LiqMesh foundation data layer:
 *  - a sent message persists and is observable (covers "send -> appears"),
 *  - duplicate ids are ignored (cross-peer dedup primitive),
 *  - status transitions work.
 */
@RunWith(AndroidJUnit4::class)
class LiqMeshFoundationTest {

    private lateinit var db: AppDatabase
    private lateinit var dao: MessageDao

    @Before
    fun setup() {
        val ctx = ApplicationProvider.getApplicationContext<android.content.Context>()
        db = Room.inMemoryDatabaseBuilder(ctx, AppDatabase::class.java).build()
        dao = db.messageDao()
    }

    @After
    fun teardown() {
        db.close()
    }

    private fun msg(id: String, body: String, at: Long) = Message(
        id = id,
        senderId = "device-self",
        senderName = "Tester",
        body = body,
        createdAt = at,
        status = MessageStatus.LOCAL,
    )

    @Test
    fun sentMessagePersistsAndIsObserved() = runTest {
        dao.insert(msg("m1", "hello mesh", 1_000L))

        val all = dao.observeAll().first()
        assertEquals(1, all.size)
        assertEquals("hello mesh", all[0].body)
        assertEquals(MessageStatus.LOCAL, all[0].status)
    }

    @Test
    fun observeAllIsOrderedByCreatedAtAscending() = runTest {
        dao.insert(msg("late", "second", 2_000L))
        dao.insert(msg("early", "first", 1_000L))

        val bodies = dao.observeAll().first().map { it.body }
        assertEquals(listOf("first", "second"), bodies)
    }

    @Test
    fun duplicateIdIsIgnored() = runTest {
        dao.insert(msg("dup", "original", 1_000L))
        dao.insert(msg("dup", "should-not-overwrite", 2_000L))

        val all = dao.observeAll().first()
        assertEquals(1, all.size)
        assertEquals("original", all[0].body)
    }

    @Test
    fun statusCanBeUpdated() = runTest {
        dao.insert(msg("s1", "body", 1_000L))
        dao.updateStatus("s1", MessageStatus.SYNCED)

        val updated = dao.observeAll().first().first()
        assertEquals(MessageStatus.SYNCED, updated.status)
        assertTrue(updated.status != MessageStatus.LOCAL)
    }
}
