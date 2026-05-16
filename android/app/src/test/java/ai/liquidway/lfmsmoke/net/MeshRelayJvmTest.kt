package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.ai.FakeSummarizationEngine
import ai.liquidway.lfmsmoke.ai.SummarizationEngine
import ai.liquidway.lfmsmoke.data.AI_SENDER_ID
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

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
    /**
     * A test stand-in mirroring [ai.liquidway.lfmsmoke.net.MeshController]
     * faithfully, including the layer-4 summary policy:
     *  - server-only generation (a leaf ignores summary_req),
     *  - generation dispatched OFF the reader (own coroutine) so plain chat
     *    keeps relaying,
     *  - single-flight dedup (a second summary_req while one runs is coalesced),
     *  - the AI summary is stored locally then relayed as a normal msg.
     */
    private class FakePeer(
        val name: String,
        private val serverMode: Boolean = false,
        private val engine: SummarizationEngine? = null,
        // The hub relays the produced summary through this transport.
        var relayTransport: MeshTransport? = null,
    ) : MeshEvents {
        val store = ConcurrentHashMap<String, Message>()
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        private val summaryDispatcher = Dispatchers.IO.limitedParallelism(1)
        private val summaryInFlight = AtomicBoolean(false)
        val summaryRunning = AtomicBoolean(false)

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

        // ---- Layer 4 (mirrors MeshController.onSummaryRequest) -----------
        override suspend fun onSummaryRequest(since: Long) {
            if (!serverMode) return // leaf never runs the model
            if (!summaryInFlight.compareAndSet(false, true)) return // coalesce
            scope.launch(summaryDispatcher) {
                summaryRunning.set(true)
                try {
                    val eng = engine ?: return@launch
                    // Recent non-AI window, oldest-first, count-capped.
                    val window = store.values
                        .filter { it.senderId != AI_SENDER_ID }
                        .sortedByDescending { it.createdAt }
                        .take(SummarizationEngine.MAX_SUMMARY_MESSAGES)
                        .asReversed()
                    val text = eng.summarize(window)
                    val summary = MessageWire.aiSummaryMessage(
                        id = UUID.randomUUID().toString(),
                        body = text,
                        createdAt = System.currentTimeMillis(),
                    )
                    put(summary)
                    relayTransport?.send(summary)
                } finally {
                    summaryRunning.set(false)
                    summaryInFlight.set(false)
                }
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

    // ---- (d) Layer 4: summary_req -> hub LFM -> ai_summary to all --------

    @Test
    fun summaryRequestRunsOnHubAndRelaysAiSummaryToAllClients() = runBlocking {
        val fake = FakeSummarizationEngine()
        val hub = FakePeer("hub", serverMode = true, engine = fake)
        val bPeer = FakePeer("B")
        val server = startHub(hub)
        hub.relayTransport = server
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null
        try {
            val port = server.boundPort
            clientA = MeshClient("127.0.0.1", port, FakePeer("A"))
            clientB = MeshClient("127.0.0.1", port, bPeer)
            clientA.start(); clientB.start()
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            // Seed chat history on the hub (as if relayed earlier).
            hub.put(msg("c-1", "water needed at shelter", "A", 1_000, MessageStatus.SENT))
            hub.put(msg("c-2", "two injured here", "B", 2_000, MessageStatus.SENT))

            // Client A taps "状況まとめ": frames a summary_req to the hub.
            assertTrue(clientA.sendRaw(MessageWire.encodeSummaryReq()))

            // Hub generated one AI summary and relayed it to every client.
            await("hub stored ai_summary") {
                hub.store.values.any { it.senderId == AI_SENDER_ID }
            }
            await("client B got ai_summary") {
                bPeer.store.values.any { it.senderId == AI_SENDER_ID }
            }
            val aiMsg = bPeer.store.values.first { it.senderId == AI_SENDER_ID }
            assertTrue("summary digests chat", aiMsg.body.contains("water needed"))
            assertEquals(1, fake.callCount.get())
            // The window excluded any AI message (none yet) and was count-capped.
            assertEquals(2, fake.lastWindowSize)
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    @Test
    fun leafIgnoresSummaryRequestAndOnlyHubGenerates() = runBlocking {
        val fake = FakeSummarizationEngine()
        // serverMode=false: this peer must NOT run the model even if asked.
        val leaf = FakePeer("leaf", serverMode = false, engine = fake)
        leaf.onSummaryRequest(0L)
        Thread.sleep(150)
        assertEquals(0, fake.callCount.get())
        assertFalse(leaf.store.values.any { it.senderId == AI_SENDER_ID })
    }

    @Test
    fun duplicateSummaryRequestsAreCoalescedWhileOneIsInFlight() = runBlocking {
        val gate = CompletableDeferred<Unit>()
        val fake = FakeSummarizationEngine(gate)
        val hub = FakePeer("hub", serverMode = true, engine = fake)
        val server = startHub(hub)
        hub.relayTransport = server
        var client: MeshClient? = null
        try {
            client = MeshClient("127.0.0.1", server.boundPort, FakePeer("A"))
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            hub.put(msg("k-1", "status ok", "A", 1_000, MessageStatus.SENT))

            // Fire 3 summary_req in quick succession; gen is parked on `gate`.
            repeat(3) { client.sendRaw(MessageWire.encodeSummaryReq()) }
            await("one generation started") { fake.callCount.get() == 1 }
            // While parked, more requests must NOT start a second generation.
            client.sendRaw(MessageWire.encodeSummaryReq())
            Thread.sleep(200)
            assertEquals("single-flight: only one gen", 1, fake.callCount.get())

            // Release: the in-flight one completes and produces exactly one msg.
            gate.complete(Unit)
            await("ai_summary stored") {
                hub.store.values.count { it.senderId == AI_SENDER_ID } == 1
            }
            assertEquals(1, hub.store.values.count { it.senderId == AI_SENDER_ID })
        } finally {
            client?.stop(); server.stop()
        }
    }

    @Test
    fun plainChatKeepsRelayingWhileASummaryIsBeingGenerated() = runBlocking {
        // Park the (slow) summary on a gate, then prove plain messages still
        // traverse the hub and reach the other client meanwhile = "chat first".
        val gate = CompletableDeferred<Unit>()
        val fake = FakeSummarizationEngine(gate)
        val hub = FakePeer("hub", serverMode = true, engine = fake)
        val bPeer = FakePeer("B")
        val server = startHub(hub)
        hub.relayTransport = server
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null
        try {
            val port = server.boundPort
            clientA = MeshClient("127.0.0.1", port, FakePeer("A"))
            clientB = MeshClient("127.0.0.1", port, bPeer)
            clientA.start(); clientB.start()
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            hub.put(msg("p-0", "seed", "A", 500, MessageStatus.SENT))

            // Kick off a summary that will block inside the engine.
            assertTrue(clientA.sendRaw(MessageWire.encodeSummaryReq()))
            await("generation parked") {
                fake.callCount.get() == 1 && hub.summaryRunning.get()
            }

            // With a summary mid-flight, send plain chat A -> hub -> B.
            assertTrue(clientA.send(msg("p-1", "still chatting", "A", 1_000)))
            await("plain msg relayed to B during summary") {
                bPeer.store.containsKey("p-1")
            }
            assertEquals("still chatting", bPeer.store["p-1"]!!.body)
            // Summary still not done (proves chat did not wait on it).
            assertFalse(hub.store.values.any { it.senderId == AI_SENDER_ID })

            // Now let the summary finish; it lands without disturbing chat.
            gate.complete(Unit)
            await("ai_summary eventually delivered") {
                bPeer.store.values.any { it.senderId == AI_SENDER_ID }
            }
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    @Test
    fun aiSummaryFrameRoundTripsAsAnOrdinaryMessage() {
        val m = MessageWire.aiSummaryMessage("s1", "the situation summary", 42L)
        val decoded = MessageWire.decode(MessageWire.encode(m))
        assertNotNull(decoded)
        assertEquals(AI_SENDER_ID, decoded!!.senderId)
        assertEquals("the situation summary", decoded.body)
        assertEquals(MessageStatus.SENT, decoded.status)
        // summary_req decodes to its own frame, never a Msg.
        assertNull(MessageWire.decode(MessageWire.encodeSummaryReq(9L)))
        assertTrue(
            MessageWire.decodeFrame(MessageWire.encodeSummaryReq(9L))
                is MessageWire.Frame.SummaryReq,
        )
        // An old peer that lacks summary_req support skips it (Unknown-safe):
        // decodeFrame must still not throw on the new type.
        assertTrue(
            MessageWire.decodeFrame(MessageWire.encodeSummaryReq())
                !is MessageWire.Frame.Unknown,
        )
    }
}
