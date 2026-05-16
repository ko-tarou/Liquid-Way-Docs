package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * Deterministic, emulator-independent proof of the LiqMesh layer-2 star relay.
 *
 * Runs the *real* [MeshServer] and [MeshClient] over real JVM loopback sockets
 * on the host JVM (not Android), so it is immune to the Android emulator's
 * unreliable user-mode loopback stack (which intermittently RSTs in-process
 * fixed/ephemeral-port connects — observed extensively during development).
 *
 * The hub's persistence is modelled with a plain id-keyed map that mirrors the
 * production DAO's OnConflict.IGNORE dedup contract, so the relay decision
 * logic is exercised exactly as in production.
 */
class MeshRelayJvmTest {

    private fun msg(id: String, body: String, sender: String = "device-A") = Message(
        id = id,
        senderId = sender,
        senderName = "Alice",
        body = body,
        createdAt = 1_700_000_000_000L,
        status = MessageStatus.LOCAL,
    )

    private fun await(desc: String, timeoutMs: Long = 5_000, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!cond()) {
            if (System.currentTimeMillis() > deadline) {
                throw AssertionError("Timed out waiting for: $desc")
            }
            Thread.sleep(20)
        }
    }

    @Test
    fun clientAMessageReachesHubStoreAndClientB() = runBlocking {
        // In-memory hub store with the production dedup contract.
        val store = ConcurrentHashMap<String, Message>()
        val bInbox = ConcurrentLinkedQueue<Message>()

        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", onInbound = { m ->
            store.putIfAbsent(m.id, m) == null // true == newly stored
        })
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null

        try {
            server.start()
            await("hub listening") { server.state.value is MeshState.Hub }
            // Let the accept-loop coroutine actually reach accept() before
            // dialing in (state flips to Hub at bind time, just before the
            // accept loop is scheduled).
            Thread.sleep(200)
            val port = server.boundPort
            assertTrue("server bound to a port", port > 0)

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

            assertTrue(clientA.send(msg("relay-1", "hello from A")))

            await("hub stored") { store.containsKey("relay-1") }
            await("client B received relay") { bInbox.any { it.id == "relay-1" } }

            assertEquals("hello from A", store["relay-1"]!!.body)
            val atB = bInbox.first { it.id == "relay-1" }
            assertEquals("hello from A", atB.body)
            // Peer-decoded messages are always SENT, never LOCAL.
            assertEquals(MessageStatus.SENT, atB.status)
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    @Test
    fun hubDoesNotEchoMessageBackToItsSender() = runBlocking {
        val store = ConcurrentHashMap<String, Message>()
        val aInbox = ConcurrentLinkedQueue<Message>()

        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", onInbound = { m ->
            store.putIfAbsent(m.id, m) == null
        })
        var clientA: MeshClient? = null

        try {
            server.start()
            await("hub listening") { server.state.value is MeshState.Hub }
            Thread.sleep(200) // let accept() be reached (see other test)
            clientA = MeshClient(host = "127.0.0.1", port = server.boundPort, onInbound = { m ->
                aInbox.add(m); true
            })
            clientA.start()
            await("1 peer") {
                val s = server.state.value
                s is MeshState.Hub && s.peerCount == 1
            }

            assertTrue(clientA.send(msg("solo-1", "only me")))
            await("hub stored") { store.containsKey("solo-1") }
            Thread.sleep(300)
            assertTrue("sender must not receive its own relay", aInbox.isEmpty())
        } finally {
            clientA?.stop(); server.stop()
        }
    }

    @Test
    fun wireRoundTripPreservesFieldsAndRejectsGarbage() {
        val original = msg("w1", "round trip")
        val decoded = MessageWire.decode(MessageWire.encode(original))
        assertNotNull(decoded)
        assertEquals("w1", decoded!!.id)
        assertEquals("round trip", decoded.body)
        assertEquals("device-A", decoded.senderId)
        assertEquals(1_700_000_000_000L, decoded.createdAt)
        assertEquals(MessageStatus.SENT, decoded.status)
        assertNull(MessageWire.decode("{ not json"))
        assertNull(MessageWire.decode(""))
    }
}
