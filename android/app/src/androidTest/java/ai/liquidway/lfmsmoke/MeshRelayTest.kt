package ai.liquidway.lfmsmoke

import ai.liquidway.lfmsmoke.data.AppDatabase
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import ai.liquidway.lfmsmoke.net.MeshClient
import ai.liquidway.lfmsmoke.net.MeshEvents
import ai.liquidway.lfmsmoke.net.MeshServer
import ai.liquidway.lfmsmoke.net.MeshState
import ai.liquidway.lfmsmoke.net.MessageWire
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/**
 * On-device proof over real loopback sockets, backed by real Room so the
 * OnConflict.IGNORE dedup + monotonic-status guard are exercised for real:
 *
 *  - layer-2 relay (client A -> hub -> client B),
 *  - layer-3 backfill (a late-joining leaf catches up its missed history,
 *    written through the production DAO so duplicate sync_resp rows collapse).
 *
 * It does NOT prove cross-emulator delivery: two emulators each sit behind
 * their own user-mode SLIRP NAT and cannot reach each other's TCP listener.
 * Real two-device same-Wi-Fi is the production validation path (see PR).
 */
@RunWith(AndroidJUnit4::class)
class MeshRelayTest {

    private lateinit var hubDb: AppDatabase
    private lateinit var leafDb: AppDatabase

    @Before
    fun setup() {
        val ctx = ApplicationProvider.getApplicationContext<android.content.Context>()
        hubDb = Room.inMemoryDatabaseBuilder(ctx, AppDatabase::class.java).build()
        leafDb = Room.inMemoryDatabaseBuilder(ctx, AppDatabase::class.java).build()
    }

    @After
    fun teardown() {
        hubDb.close()
        leafDb.close()
    }

    private fun msg(
        id: String,
        body: String,
        createdAt: Long = 1_700_000_000_000L,
        status: MessageStatus = MessageStatus.LOCAL,
    ) = Message(id, "device-A", "Alice", body, createdAt, status)

    private fun await(desc: String, timeoutMs: Long = 10_000, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!cond()) {
            if (System.currentTimeMillis() > deadline) {
                throw AssertionError("Timed out waiting for: $desc")
            }
            Thread.sleep(50)
        }
    }

    /** Room-backed peer policy (subset of MeshController, no DataStore). */
    private inner class DbPeer(private val db: AppDatabase) : MeshEvents {
        private val dao = db.messageDao()
        override suspend fun onMessage(message: Message): Boolean =
            dao.insertReturning(message) != -1L

        override suspend fun onSyncRequest(since: Long, reply: suspend (String) -> Unit) {
            dao.recentSince(since, 500).asReversed()
                .forEach { reply(MessageWire.encodeSyncResp(it)) }
        }

        override suspend fun onLinkEstablished(
            transport: ai.liquidway.lfmsmoke.net.MeshTransport,
        ) {
            if (transport is MeshClient) {
                val since = dao.maxSyncedCreatedAt() ?: 0L
                transport.sendRaw(MessageWire.encodeSyncReq(since))
                dao.pendingOutbox().forEach {
                    if (transport.send(it)) dao.updateStatus(it.id, MessageStatus.SENT)
                }
            }
        }

        // Layer-4 hook: this layer-2/3 instrumented test does not exercise AI
        // summarisation (covered deterministically in MeshRelayJvmTest), so a
        // no-op satisfies the interface without changing this test's scope.
        override suspend fun onSummaryRequest(since: Long) = Unit
    }

    @Test
    fun clientAMessageReachesHubDbAndClientB() = runBlocking {
        val hubDao = hubDb.messageDao()
        val bDb = Room.inMemoryDatabaseBuilder(
            ApplicationProvider.getApplicationContext(),
            AppDatabase::class.java,
        ).build()
        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", events = DbPeer(hubDb))
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null
        try {
            server.start()
            await("hub listening") { server.state.value is MeshState.Hub }
            Thread.sleep(300)
            val port = server.boundPort
            assertTrue("server should have a bound port", port > 0)
            clientA = MeshClient("127.0.0.1", port, DbPeer(leafDb))
            clientB = MeshClient("127.0.0.1", port, DbPeer(bDb))
            clientA.start(); clientB.start()
            await("2 peers connected") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            val sent = msg("relay-1", "hello from A")
            assertTrue(clientA.send(sent))
            await("hub stored") {
                runBlocking { hubDao.observeAllNow() }.any { it.id == "relay-1" }
            }
            await("client B received relay") {
                runBlocking { bDb.messageDao().observeAllNow() }.any { it.id == "relay-1" }
            }
            val atB = runBlocking { bDb.messageDao().observeAllNow() }.first { it.id == "relay-1" }
            assertEquals(MessageStatus.SENT, atB.status)
            bDb.close()
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    @Test
    fun lateLeafBackfillsMissedHistoryThroughRoomDedup() = runBlocking {
        val hubDao = hubDb.messageDao()
        // Hub history authored before the leaf existed.
        hubDao.insert(msg("h-1", "old 1", 1_000, MessageStatus.SENT))
        hubDao.insert(msg("h-2", "old 2", 2_000, MessageStatus.SENT))
        hubDao.insert(msg("h-3", "old 3", 3_000, MessageStatus.SENT))

        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", events = DbPeer(hubDb))
        var client: MeshClient? = null
        try {
            server.start()
            await("hub up") { server.state.value is MeshState.Hub }
            Thread.sleep(300)
            client = MeshClient("127.0.0.1", server.boundPort, DbPeer(leafDb))
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            await("leaf backfilled 3") {
                runBlocking { leafDb.messageDao().observeAllNow() }.size == 3
            }
            // Replay again: real DAO OnConflict.IGNORE keeps it at 3 rows.
            client.sendRaw(MessageWire.encodeSyncReq(0L))
            Thread.sleep(400)
            assertEquals(3, runBlocking { leafDb.messageDao().observeAllNow() }.size)
        } finally {
            client?.stop(); server.stop()
        }
    }
}
