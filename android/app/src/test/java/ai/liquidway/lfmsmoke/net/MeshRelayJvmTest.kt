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

/**
 * Deterministic, emulator-independent proof of the LiqMesh star relay AND the
 * layer-3 offline sync (outbox flush + reconnect resend + history backfill).
 *
 * Runs the *real* [MeshServer] and [MeshClient] over real JVM loopback sockets
 * on the host JVM (not Android), so it is immune to the Android emulator's
 * unreliable user-mode loopback stack (which intermittently RSTs in-process
 * connects — observed extensively during layer-2 development) AND to the cross
 * -emulator NAT limitation (two emulators cannot reach each other's TCP sockets
 * because each sits behind its own user-mode SLIRP NAT; real two-device LAN is
 * the production validation path). The protocol/dedup logic exercised here is
 * exactly the production code.
 *
 * [FakePeer] models one device's persistence + outbox + backfill policy with an
 * id-keyed map honouring the production OnConflict.IGNORE + monotonic-status
 * contract, so [MeshController]'s logic is exercised faithfully without Room.
 */
class MeshRelayJvmTest {

    private fun msg(
        id: String,
        body: String,
        sender: String = "device-A",
        createdAt: Long = 1_700_000_000_000L,
        status: MessageStatus = MessageStatus.LOCAL,
    ) = Message(id, sender, "Alice", body, createdAt, status)

    private fun await(desc: String, timeoutMs: Long = 5_000, cond: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (!cond()) {
            if (System.currentTimeMillis() > deadline) {
                throw AssertionError("Timed out waiting for: $desc")
            }
            Thread.sleep(20)
        }
    }

    /**
     * A test stand-in for one device's repository + controller policy. Mirrors:
     *  - DAO OnConflict.IGNORE dedup (insert returns "isNew"),
     *  - monotonic status (LOCAL < SENT < SYNCED; never demote),
     *  - the controller's backfill answer + leaf sync_req/outbox-on-connect.
     */
    private class FakePeer(val name: String) : MeshEvents {
        val store = ConcurrentHashMap<String, Message>()

        fun put(m: Message): Boolean {
            val prev = store.putIfAbsent(m.id, m)
            return prev == null
        }

        fun markSent(id: String) {
            store.computeIfPresent(id) { _, cur ->
                if (cur.status == MessageStatus.LOCAL) cur.copy(status = MessageStatus.SENT) else cur
            }
        }

        override suspend fun onMessage(message: Message): Boolean = put(message)

        override suspend fun onSyncRequest(since: Long, reply: suspend (String) -> Unit) {
            store.values
                .filter { it.createdAt > since }
                .sortedBy { it.createdAt }
                .takeLast(500)
                .forEach { reply(MessageWire.encodeSyncResp(it)) }
        }

        override suspend fun onLinkEstablished(transport: MeshTransport) {
            if (transport is MeshClient) {
                val since = store.values
                    .filter { it.status != MessageStatus.LOCAL }
                    .maxOfOrNull { it.createdAt } ?: 0L
                transport.sendRaw(MessageWire.encodeSyncReq(since))
                // Flush outbox: LOCAL rows in createdAt order; stop at failure.
                store.values
                    .filter { it.status == MessageStatus.LOCAL }
                    .sortedBy { it.createdAt }
                    .forEach { if (transport.send(it)) markSent(it.id) }
            }
        }
    }

    private fun startHub(hub: FakePeer): MeshServer {
        val server = MeshServer(port = 0, bindAddress = "127.0.0.1", events = hub)
        runBlocking { server.start() }
        await("hub listening") { server.state.value is MeshState.Hub }
        Thread.sleep(200) // let the accept loop actually reach accept()
        return server
    }

    // ---- Layer-2 regression (kept; new constructor) ----------------------

    @Test
    fun clientAMessageReachesHubStoreAndClientB() = runBlocking {
        val hub = FakePeer("hub")
        val bPeer = FakePeer("B")
        val server = startHub(hub)
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null
        try {
            val port = server.boundPort
            assertTrue("server bound", port > 0)
            clientA = MeshClient("127.0.0.1", port, FakePeer("A"))
            clientB = MeshClient("127.0.0.1", port, bPeer)
            clientA.start(); clientB.start()
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            assertTrue(clientA.send(msg("relay-1", "hello from A")))
            await("hub stored") { hub.store.containsKey("relay-1") }
            await("B received") { bPeer.store.containsKey("relay-1") }
            assertEquals(MessageStatus.SENT, bPeer.store["relay-1"]!!.status)
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    @Test
    fun wireRoundTripPreservesFieldsAndRejectsGarbage() {
        val decoded = MessageWire.decode(MessageWire.encode(msg("w1", "round trip")))
        assertNotNull(decoded)
        assertEquals("w1", decoded!!.id)
        assertEquals("round trip", decoded.body)
        assertEquals(MessageStatus.SENT, decoded.status)
        assertNull(MessageWire.decode("{ not json"))
        assertNull(MessageWire.decode(""))
        // Backward compat: a legacy frame with NO type field decodes as a msg.
        val legacy = """{"id":"L1","senderId":"d","senderName":"n","body":"b","createdAt":1}"""
        assertEquals("L1", MessageWire.decode(legacy)!!.id)
        // sync_req frame is NOT a Msg.
        assertNull(MessageWire.decode(MessageWire.encodeSyncReq(123L)))
        assertTrue(MessageWire.decodeFrame(MessageWire.encodeSyncReq(7L))
            is MessageWire.Frame.SyncReq)
    }

    // ---- (a) Outbox: offline sends flush in order on connect -------------

    @Test
    fun outboxFlushesQueuedMessagesInOrderOnConnect() = runBlocking {
        val hub = FakePeer("hub")
        val leaf = FakePeer("leaf")
        // Author 3 messages while the leaf has no link: all stay LOCAL.
        val m1 = msg("o-1", "first", "L", 1_000)
        val m2 = msg("o-2", "second", "L", 2_000)
        val m3 = msg("o-3", "third", "L", 3_000)
        leaf.put(m1); leaf.put(m2); leaf.put(m3)
        assertTrue(leaf.store.values.all { it.status == MessageStatus.LOCAL })

        val server = startHub(hub)
        var client: MeshClient? = null
        try {
            client = MeshClient("127.0.0.1", server.boundPort, leaf)
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            // onLinkEstablished flushed the outbox; hub must receive all 3.
            await("hub got all 3") {
                hub.store.keys.containsAll(listOf("o-1", "o-2", "o-3"))
            }
            // All flipped LOCAL -> SENT on the leaf, none duplicated.
            await("all SENT") {
                leaf.store.values.all { it.status == MessageStatus.SENT }
            }
            assertEquals(3, hub.store.size)
            assertEquals("first", hub.store["o-1"]!!.body)
            assertEquals("third", hub.store["o-3"]!!.body)
            // Idempotency: flushing again sends nothing new / no dup rows.
            leaf.onLinkEstablished(client)
            Thread.sleep(150)
            assertEquals(3, hub.store.size)
        } finally {
            client?.stop(); server.stop()
        }
    }

    // ---- (b) Disconnect, send more, reconnect: nothing lost, deduped -----

    @Test
    fun reconnectResendsWithoutLossOrDuplication() = runBlocking {
        val hub = FakePeer("hub")
        val leaf = FakePeer("leaf")
        val server = startHub(hub)
        try {
            // Phase 1: connect, send one, it lands.
            var client = MeshClient("127.0.0.1", server.boundPort, leaf)
            client.start()
            await("connected #1") { client.state.value is MeshState.Connected }
            val first = msg("r-1", "before drop", "L", 1_000)
            leaf.put(first)
            assertTrue(client.send(first)); leaf.markSent("r-1")
            await("hub got r-1") { hub.store.containsKey("r-1") }

            // Drop the link, then author 2 more while offline (stay LOCAL).
            client.stop()
            leaf.put(msg("r-2", "while offline A", "L", 2_000))
            leaf.put(msg("r-3", "while offline B", "L", 3_000))

            // Phase 2: reconnect -> outbox flush replays only the LOCAL ones.
            client = MeshClient("127.0.0.1", server.boundPort, leaf)
            client.start()
            await("connected #2") { client.state.value is MeshState.Connected }
            await("hub got r-2 & r-3") {
                hub.store.keys.containsAll(listOf("r-2", "r-3"))
            }
            // No loss, no duplication, r-1 not re-sent (already SENT).
            assertEquals(3, hub.store.size)
            assertTrue(leaf.store.values.all { it.status == MessageStatus.SENT })
            client.stop()
        } finally {
            server.stop()
        }
    }

    // ---- (c) Backfill: late joiner receives messages it missed ----------

    @Test
    fun lateJoinerBackfillsMissedHistoryFromHub() = runBlocking {
        val hub = FakePeer("hub")
        // Hub already holds history authored before the leaf ever connected.
        hub.put(msg("h-1", "old 1", "X", 1_000, MessageStatus.SENT))
        hub.put(msg("h-2", "old 2", "X", 2_000, MessageStatus.SENT))
        hub.put(msg("h-3", "old 3", "X", 3_000, MessageStatus.SENT))

        val leaf = FakePeer("leaf") // empty: store is fresh, watermark 0.
        val server = startHub(hub)
        var client: MeshClient? = null
        try {
            client = MeshClient("127.0.0.1", server.boundPort, leaf)
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            // Leaf sent sync_req(since=0); hub replies sync_resp for h-1..h-3.
            await("leaf backfilled all 3") {
                leaf.store.keys.containsAll(listOf("h-1", "h-2", "h-3"))
            }
            assertEquals("old 2", leaf.store["h-2"]!!.body)
            // Backfilled rows arrive as SENT (peer-decoded), never LOCAL.
            assertTrue(leaf.store.values.all { it.status == MessageStatus.SENT })
            // Idempotent: a second sync_req does not duplicate rows.
            client.sendRaw(MessageWire.encodeSyncReq(0L))
            Thread.sleep(200)
            assertEquals(3, leaf.store.size)
        } finally {
            client?.stop(); server.stop()
        }
    }
}
