package ai.liquidway.lfmsmoke

import ai.liquidway.lfmsmoke.data.AppDatabase
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import ai.liquidway.lfmsmoke.net.MeshClient
import ai.liquidway.lfmsmoke.net.MeshServer
import ai.liquidway.lfmsmoke.net.MeshState
import ai.liquidway.lfmsmoke.net.MessageWire
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Deterministic proof of the layer-2 star relay over real loopback sockets:
 *
 *   client A --[msg]--> hub --(persist + relay)--> client B
 *
 * Runs entirely on 127.0.0.1 inside one process, so it is a true end-to-end
 * socket test (not a mock). It does NOT prove cross-emulator delivery — see the
 * NAT note in the PR description.
 */
@RunWith(AndroidJUnit4::class)
class MeshRelayTest {

    private lateinit var hubDb: AppDatabase

    @Before
    fun setup() {
        val ctx = ApplicationProvider.getApplicationContext<android.content.Context>()
        hubDb = Room.inMemoryDatabaseBuilder(ctx, AppDatabase::class.java).build()
    }

    @After
    fun teardown() {
        hubDb.close()
    }

    private fun msg(id: String, body: String) = Message(
        id = id,
        senderId = "device-A",
        senderName = "Alice",
        body = body,
        createdAt = 1_700_000_000_000L,
        status = MessageStatus.LOCAL,
    )

    /**
     * Spins on a condition using real wall-clock sleep (NOT coroutine delay):
     * under instrumentation `runBlocking { ... delay() }` does not reliably
     * advance real time relative to the socket coroutines on Dispatchers.IO,
     * so a blocking sleep is the deterministic primitive here.
     */
    private fun await(desc: String, timeoutMs: Long = 10_000, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!cond()) {
            if (System.currentTimeMillis() > deadline) {
                throw AssertionError("Timed out waiting for: $desc")
            }
            Thread.sleep(50)
        }
    }

    @Test
    fun clientAMessageReachesHubDbAndClientB() = runBlocking {
        val hubDao = hubDb.messageDao()
        val bInbox = ConcurrentLinkedQueue<Message>()

        // Hub: persist inbound to its Room db, then relay to other clients.
        // Port 0 = OS-assigned ephemeral port so parallel test methods never
        // collide on a fixed port. Production binds the fixed default port.
        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", onInbound = { m ->
            val rowId = hubDao.insertReturning(m)
            rowId != -1L
        })
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null

        try {
            server.start()
            await("hub listening") { server.state.value is MeshState.Hub }
            Thread.sleep(300)
            val port = server.boundPort
            assertTrue("server should have a bound port", port > 0)

            clientA = MeshClient(host = "127.0.0.1", port = port, onInbound = { true })
            clientB = MeshClient(host = "127.0.0.1", port = port, onInbound = { m ->
                bInbox.add(m); true
            })

            clientA.start()
            clientB.start()
            await("2 peers connected") {
                val s = server.state.value
                s is MeshState.Hub && s.peerCount == 2
            }

            val sent = msg("relay-1", "hello from A")
            assertTrue("clientA.send should hand bytes to the socket", clientA.send(sent))

            // (a) hub persisted it
            await("hub stored the message") {
                runBlocking { hubDao.observeAllNow() }.any { it.id == "relay-1" }
            }
            // (b) client B received the relayed copy
            await("client B received relay") { bInbox.any { it.id == "relay-1" } }

            val onHub = runBlocking { hubDao.observeAllNow() }.first { it.id == "relay-1" }
            assertEquals("hello from A", onHub.body)
            val atB = bInbox.first { it.id == "relay-1" }
            assertEquals("hello from A", atB.body)
            // Decoded peer messages are materialised as SENT, never LOCAL.
            assertEquals(MessageStatus.SENT, atB.status)
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    @Test
    fun senderDoesNotReceiveItsOwnRelay() = runBlocking {
        val hubDao = hubDb.messageDao()
        val aInbox = ConcurrentLinkedQueue<Message>()

        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", onInbound = { m ->
            hubDao.insertReturning(m) != -1L
        })
        var clientA: MeshClient? = null

        try {
            server.start()
            await("hub up") { server.state.value is MeshState.Hub }
            Thread.sleep(300)
            val port = server.boundPort
            assertTrue("server should have a bound port", port > 0)
            clientA = MeshClient(host = "127.0.0.1", port = port, onInbound = { m ->
                aInbox.add(m); true
            })
            clientA.start()
            await("1 peer") {
                val s = server.state.value
                s is MeshState.Hub && s.peerCount == 1
            }

            assertTrue(clientA!!.send(msg("solo-1", "only me")))
            await("hub stored") { runBlocking { hubDao.observeAllNow() }.any { it.id == "solo-1" } }
            // The hub must NOT echo the message back to its author.
            Thread.sleep(500)
            assertTrue("sender should not get its own message back", aInbox.isEmpty())
        } finally {
            clientA?.stop(); server.stop()
        }
    }

    @Test
    fun wireRoundTripPreservesFields() {
        val original = msg("w1", "round trip")
        val line = MessageWire.encode(original)
        val decoded = MessageWire.decode(line)
        assertNotNull(decoded)
        assertEquals("w1", decoded!!.id)
        assertEquals("round trip", decoded.body)
        assertEquals("device-A", decoded.senderId)
        assertEquals(1_700_000_000_000L, decoded.createdAt)
        // status is local-only; the wire always yields SENT.
        assertEquals(MessageStatus.SENT, decoded.status)
        assertEquals(null, MessageWire.decode("{ not json"))
    }
}
