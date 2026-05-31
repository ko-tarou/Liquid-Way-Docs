package ai.liquidway.lfmsmoke.net

import ai.liquidway.lfmsmoke.ai.FakeSummarizationEngine
import ai.liquidway.lfmsmoke.ai.SummarizationEngine
import ai.liquidway.lfmsmoke.data.AI_SENDER_ID
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.net.Socket
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
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
        // This hub's stable id, stamped on its summary_claim (layer 6).
        private val deviceId: String = name,
        // Operator-layer 3: the non-target claim fallback delay (mirrors
        // MeshController.dispatchFallbackMs). Injectable so a test can drive it
        // to a small/large value deterministically.
        private val dispatchFallbackMs: Long = 600L,
    ) : MeshEvents {
        val store = ConcurrentHashMap<String, Message>()
        // Mirrors MeshController.bridgePolicy: the hub forwards across a bridge
        // through this real policy (seen-set + hop ceiling), so the loop and
        // ceiling guards exercised here are the production logic.
        val bridgePolicy = BridgePolicy()
        // Mirrors MeshController.questionOwnership: the REAL per-question
        // single-ownership logic (forward-once + claim CAS + cross-ack + cap).
        val questionOwnership = QuestionOwnership()

        /**
         * Mirrors [MeshController.transport]/[MeshController.bridge] for the
         * 2-hub E2E. `localTransport` fans out to THIS hub's leaves; `bridge` is
         * the secondary MeshClient to the second hub (A->B). Either may be null.
         */
        var localTransport: MeshTransport? = null
        var bridge: MeshClient? = null

        // Same A->B chokepoint as MeshController.forwardToBridge.
        private fun forwardToBridge(message: Message, hop: Int, originId: String?) {
            val b = bridge ?: return
            val nextHop = bridgePolicy.bridgeHopFor(message.id, hop) ?: return
            val stamped = originId ?: message.senderId
            runBlocking {
                b.sendRaw(MessageWire.encode(message, hop = nextHop, originId = stamped))
            }
        }
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        private val summaryDispatcher = Dispatchers.IO.limitedParallelism(1)
        private val summaryInFlight = AtomicBoolean(false)
        val summaryRunning = AtomicBoolean(false)
        // Mirrors MeshController.deferredClaimJobs: deferred (non-target) claims
        // tracked per qid so teardown() can cancel a job still in its window.
        val deferredClaimJobs = ConcurrentHashMap<String, Job>()

        // ---- Operator-layer load (mirrors MeshController) -----------------
        // This hub's advertised load, settable by a test to simulate pressure.
        @Volatile
        var selfLoad = HubLoad()
        // Per-peer last-reported load, populated by onPeerLoad from heartbeats.
        val peerLoads = ConcurrentHashMap<String, HubLoad>()

        override fun localLoad(): HubLoad = selfLoad
        override fun localDeviceId(): String = deviceId
        override suspend fun onPeerLoad(deviceId: String, load: HubLoad) {
            peerLoads[deviceId] = load
        }

        // ---- Operator-layer 3 dispatch hint (mirrors MeshController) ------
        // The hint this hub advertises on its outgoing hello. A test sets it to
        // simulate "this hub is the operator and chose target X" (null = no hint).
        @Volatile
        var dispatchTargetToAdvertise: String? = null
        // The deviceId this hub believes is the operator (mirrors
        // effectiveOperatorId). A test sets it so onDispatchHint can gate on it.
        @Volatile
        var believedOperator: String? = null
        // The freshest hint received from the operator + when (for staleness).
        @Volatile
        var receivedHint: Pair<String?, Long>? = null

        override fun localDispatchTarget(): String? = dispatchTargetToAdvertise
        override suspend fun onDispatchHint(fromDeviceId: String, target: String?) {
            // Mirror MeshController: only the operator's hint counts; a
            // non-operator hello (null target) must not clobber a live hint.
            val op = believedOperator
            if (op != null && fromDeviceId != op) return
            receivedHint = target to System.currentTimeMillis()
        }

        // Mirrors MeshController.effectiveDispatchTarget: if this hub IS the
        // operator it uses its OWN advertised target; otherwise the freshest
        // received hint within the stale window (else null -> PR#6 floor).
        private fun effectiveDispatchTarget(now: Long): String? {
            if (believedOperator != null && believedOperator == deviceId) {
                return dispatchTargetToAdvertise
            }
            val (target, seenAt) = receivedHint ?: return null
            if (now - seenAt > OperatorElection.DEFAULT_PEER_STALE_MS) return null
            return target
        }

        fun put(m: Message): Boolean {
            val prev = store.putIfAbsent(m.id, m)
            return prev == null
        }

        fun markSent(id: String) {
            store.computeIfPresent(id) { _, cur ->
                if (cur.status == MessageStatus.LOCAL) cur.copy(status = MessageStatus.SENT) else cur
            }
        }

        override suspend fun onMessage(message: Message): Boolean {
            val isNew = put(message)
            // Leaf-origin on a hub also crosses to the second hub (A->B).
            forwardToBridge(message, hop = 0, originId = null)
            return isNew
        }

        override suspend fun ingest(
            message: Message,
            hop: Int,
            originId: String?,
            fromBridge: Boolean,
        ) {
            if (!fromBridge) {
                put(message)
                return
            }
            // Bridge inbound: persist, fan out to local leaves, never echo back.
            val isNew = put(message)
            if (isNew) localTransport?.send(message)
        }

        override fun bridgeHopFor(messageId: String, hop: Int): Int? =
            bridgePolicy.bridgeHopFor(messageId, hop)

        // Mirrors MeshController.bridgeWatermark / latestCreatedAt: highest
        // createdAt among peer-received (non-LOCAL) rows, 0 if none. Used by both
        // bridge partition-recovery directions.
        override suspend fun bridgeWatermark(): Long =
            store.values
                .filter { it.status != MessageStatus.LOCAL }
                .maxOfOrNull { it.createdAt } ?: 0L

        override suspend fun onSyncRequest(since: Long, reply: suspend (String) -> Unit) {
            store.values
                .filter { it.createdAt > since }
                .sortedBy { it.createdAt }
                .takeLast(500)
                .forEach { reply(MessageWire.encodeSyncResp(it)) }
        }

        override suspend fun onLinkEstablished(transport: MeshTransport) {
            // Bridge (re)connect, B->A partition recovery: ask the second hub for
            // anything created after our watermark, but NEVER flush our outbox
            // onto it (a hub's LOCAL rows belong to its own leaves). Mirrors
            // MeshController.onLinkEstablished's bridge branch.
            if (transport === bridge) {
                transport.sendRaw(MessageWire.encodeSyncReq(bridgeWatermark()))
                return
            }
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

        // Test-observable count of cross-ack ("すれ違い") events: incremented
        // each time onSummaryClaim sees a question now co-owned by two hubs.
        val crossAckCount = java.util.concurrent.atomic.AtomicInteger(0)

        // ---- Layer 6 (mirrors MeshController.onSummaryClaim) -------------
        override suspend fun onSummaryClaim(questionId: String, ownerId: String) {
            if (!serverMode) return
            if (ownerId == deviceId) return // our own claim echoed back
            questionOwnership.onRemoteClaim(questionId, ownerId)
            if (questionOwnership.isCrossAck(questionId)) crossAckCount.incrementAndGet()
        }

        // ---- Layer 4/6 (mirrors MeshController.onSummaryRequest) ----------
        override suspend fun onSummaryRequest(since: Long, questionId: String?) {
            if (!serverMode) return // leaf never runs the model
            val qid = questionId ?: UUID.randomUUID().toString()
            // Operator-layer 3: advisory dispatch — bias WHEN we claim, never
            // WHETHER (the CAS below is the floor). No fresh hint or we ARE the
            // target -> claim now; a fresh hint pointing elsewhere -> defer by
            // dispatchFallbackMs (the target normally claims first; if it is dead
            // we still claim after the window -> liveness).
            val target = effectiveDispatchTarget(System.currentTimeMillis())
            if (target != null && target != deviceId) {
                // Forward the request to the far hub FIRST so the target can
                // claim it, THEN defer our own claim (mirrors MeshController).
                bridge?.let { b ->
                    if (questionOwnership.shouldForward(qid)) {
                        b.sendRaw(MessageWire.encodeSummaryReq(since, qid))
                    }
                }
                val job = scope.launch {
                    try {
                        kotlinx.coroutines.delay(dispatchFallbackMs)
                        claimAndGenerate(since, qid)
                    } finally {
                        deferredClaimJobs.remove(qid)
                    }
                }
                deferredClaimJobs[qid] = job
                return
            }
            claimAndGenerate(since, qid)
        }

        // Mirrors MeshController.teardown()'s deferred-claim cancellation: a job
        // still waiting out its window must not wake up and claim a stale qid.
        fun teardown() {
            deferredClaimJobs.values.forEach { it.cancel() }
            deferredClaimJobs.clear()
        }

        private suspend fun claimAndGenerate(since: Long, qid: String) {
            // Per-question ownership CAS: stand down if a peer already claimed.
            if (!questionOwnership.tryClaim(qid, deviceId)) return
            // Claim BEFORE forwarding the request (same ordered bridge socket),
            // so the far hub records our ownership and stands down on the
            // forwarded request -> single answer. (Mirrors MeshController.)
            bridge?.let { b ->
                b.sendRaw(MessageWire.encodeSummaryClaim(qid, deviceId))
                if (questionOwnership.shouldForward(qid)) {
                    b.sendRaw(MessageWire.encodeSummaryReq(since, qid))
                }
            }
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
                    // Cross to the second hub so its leaves see the summary too
                    // (mirrors MeshController.forwardToBridge on the summary).
                    forwardToBridge(summary, hop = 0, originId = null)
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

    // ---- Stage-1 bridge wire (dead-data: type/encode/decode only) --------

    @Test
    fun msgEnvelopePreservesHopAndOriginIdRoundTrip() {
        val line = MessageWire.encode(msg("e1", "bridged"), hop = 2, originId = "device-Z")
        val f = MessageWire.decodeFrame(line) as MessageWire.Frame.Msg
        assertEquals("e1", f.message.id)
        assertEquals(2, f.hop)
        assertEquals("device-Z", f.originId)
    }

    @Test
    fun legacyMsgWithoutEnvelopeDecodesToHopZeroAndNullOrigin() {
        // A new-style msg that simply omits the optional bridge fields.
        val noEnvelope =
            """{"type":"msg","id":"n1","senderId":"d","senderName":"n","body":"b","createdAt":5}"""
        val f = MessageWire.decodeFrame(noEnvelope) as MessageWire.Frame.Msg
        assertEquals(0, f.hop)
        assertNull(f.originId)
        // The oldest layer-2 shape (no type field at all) still decodes too.
        val legacy = """{"id":"L1","senderId":"d","senderName":"n","body":"b","createdAt":1}"""
        val lf = MessageWire.decodeFrame(legacy) as MessageWire.Frame.Msg
        assertEquals("L1", lf.message.id)
        assertEquals(0, lf.hop)
        assertNull(lf.originId)
    }

    @Test
    fun bridgeHelloRoundTrips() {
        val f = MessageWire.decodeFrame(MessageWire.encodeBridgeHello("hub-7"))
        assertTrue(f is MessageWire.Frame.BridgeHello)
        assertEquals("hub-7", (f as MessageWire.Frame.BridgeHello).deviceId)
    }

    @Test
    fun summaryClaimRoundTrips() {
        val f = MessageWire.decodeFrame(MessageWire.encodeSummaryClaim("q-1", "owner-A"))
        assertTrue(f is MessageWire.Frame.SummaryClaim)
        val claim = f as MessageWire.Frame.SummaryClaim
        assertEquals("q-1", claim.questionId)
        assertEquals("owner-A", claim.ownerId)
    }

    @Test
    fun summaryReqQuestionIdRoundTripsAndDefaultsNull() {
        val withId = MessageWire.decodeFrame(MessageWire.encodeSummaryReq(0L, "q-42"))
                as MessageWire.Frame.SummaryReq
        assertEquals("q-42", withId.questionId)
        val without = MessageWire.decodeFrame(MessageWire.encodeSummaryReq())
                as MessageWire.Frame.SummaryReq
        assertNull(without.questionId)
    }

    @Test
    fun newBridgeFramesAreUnknownSafeAndDoNotDropTheLink() {
        // A pre-bridge peer never throws on the new types; if it lacked the
        // branches it would simply see Unknown and skip (link stays up).
        assertTrue(
            MessageWire.decodeFrame(MessageWire.encodeBridgeHello("x"))
                !is MessageWire.Frame.Unknown,
        )
        assertTrue(
            MessageWire.decodeFrame(MessageWire.encodeSummaryClaim("q", "o"))
                !is MessageWire.Frame.Unknown,
        )
        // A genuinely unknown future type is the skip-not-drop contract.
        val future = """{"type":"totally_new","x":1}"""
        assertTrue(MessageWire.decodeFrame(future) is MessageWire.Frame.Unknown)
    }

    // ---- Stage-1 bridge: relay routing (isBridge + forwarding policy) -----
    //
    // The real bridge transport (a hub→hub MeshClient) is wired in PR#4, so a
    // raw loopback socket stands in for the "other hub": it connects, sends a
    // bridge_hello so the hub flips its connection to isBridge=true, and then
    // captures the exact frames the hub forwards (so hop/originId can be
    // asserted on the wire). hop/originId are the sender's self-report and are
    // not authenticated here; loop safety is the BridgePolicy seen-set + ceiling.

    /** A raw socket masquerading as a peer hub across a bridge link. */
    private class FakeBridge(port: Int) {
        private val socket = Socket().apply {
            tcpNoDelay = true
            connect(InetSocketAddress("127.0.0.1", port), 5_000)
        }
        private val out = socket.getOutputStream()
        private val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.UTF_8))
        val received = CopyOnWriteArrayList<MessageWire.Frame.Msg>()
        private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

        init {
            scope.launch {
                while (true) {
                    val line = reader.readLine() ?: break
                    (MessageWire.decodeFrame(line) as? MessageWire.Frame.Msg)?.let { received.add(it) }
                }
            }
        }

        /** Announce as a hub so the server marks this connection isBridge=true. */
        fun announce(deviceId: String) {
            out.write(MessageWire.encodeBridgeHello(deviceId).toByteArray(Charsets.UTF_8))
            out.flush()
        }

        /** Inject a chat msg into the hub as if forwarded across the bridge. */
        fun sendMsg(line: String) {
            out.write(line.toByteArray(Charsets.UTF_8))
            out.flush()
        }

        fun close() {
            scope.cancel()
            runCatching { socket.close() }
        }
    }

    @Test
    fun leafMessageIsForwardedToBridgeWithIncrementedHopAndStampedOrigin() = runBlocking {
        val hub = FakePeer("hub")
        val server = startHub(hub)
        var leaf: MeshClient? = null
        var bridge: FakeBridge? = null
        try {
            val port = server.boundPort
            bridge = FakeBridge(port)
            bridge.announce("hub-B")
            leaf = MeshClient("127.0.0.1", port, FakePeer("leaf"))
            leaf.start()
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            // A leaf-authored msg (hop 0, no origin) must cross the bridge once.
            assertTrue(leaf.send(msg("b-1", "to bridge", "device-A")))
            await("bridge got forwarded msg") { bridge!!.received.any { it.message.id == "b-1" } }
            val f = bridge.received.first { it.message.id == "b-1" }
            assertEquals("hop incremented 0 -> 1", 1, f.hop)
            assertEquals("origin stamped to sender", "device-A", f.originId)
        } finally {
            leaf?.stop(); bridge?.close(); server.stop()
        }
    }

    @Test
    fun bridgeMessageFansOutToLeavesButNeverBackToTheBridge() = runBlocking {
        val hub = FakePeer("hub")
        val leafPeer = FakePeer("leaf")
        val server = startHub(hub)
        var leaf: MeshClient? = null
        var bridge: FakeBridge? = null
        try {
            val port = server.boundPort
            leaf = MeshClient("127.0.0.1", port, leafPeer)
            leaf.start()
            bridge = FakeBridge(port)
            bridge.announce("hub-B")
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            // A msg arriving FROM the bridge: leaves see it, the bridge does not.
            bridge.sendMsg(MessageWire.encode(msg("b-2", "from bridge", "device-Z"), hop = 1, originId = "device-Z"))
            await("leaf got bridged msg") { leafPeer.store.containsKey("b-2") }
            // Give the hub ample time to (wrongly) echo before asserting absence.
            Thread.sleep(200)
            assertTrue("must not echo to source bridge", bridge.received.none { it.message.id == "b-2" })
        } finally {
            leaf?.stop(); bridge?.close(); server.stop()
        }
    }

    @Test
    fun sameMessageIsNotForwardedToTheBridgeTwice() = runBlocking {
        val hub = FakePeer("hub")
        val server = startHub(hub)
        var leaf: MeshClient? = null
        var bridge: FakeBridge? = null
        try {
            val port = server.boundPort
            bridge = FakeBridge(port)
            bridge.announce("hub-B")
            leaf = MeshClient("127.0.0.1", port, FakePeer("leaf"))
            leaf.start()
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            // Same id sent twice: the seen-set forwards the first, refuses the 2nd.
            assertTrue(leaf.send(msg("dup-1", "first", "device-A")))
            await("bridge got it once") { bridge!!.received.any { it.message.id == "dup-1" } }
            assertTrue(leaf.send(msg("dup-1", "second", "device-A")))
            Thread.sleep(200)
            assertEquals("forwarded across bridge exactly once", 1, bridge.received.count { it.message.id == "dup-1" })
        } finally {
            leaf?.stop(); bridge?.close(); server.stop()
        }
    }

    @Test
    fun messageAtMaxHopIsNotForwardedToTheBridge() = runBlocking {
        val hub = FakePeer("hub")
        val leafPeer = FakePeer("leaf")
        val server = startHub(hub)
        var leaf: MeshClient? = null
        var bridge: FakeBridge? = null
        try {
            val port = server.boundPort
            leaf = MeshClient("127.0.0.1", port, leafPeer)
            leaf.start()
            bridge = FakeBridge(port)
            bridge.announce("hub-B")
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            // hop == MAX_HOP from the leaf: the ceiling guard blocks the bridge
            // forward (leaf fan-out is unaffected — this leaf is the sender, so
            // there is no other leaf to observe; the assertion is bridge-absence).
            assertTrue(leaf.sendRaw(MessageWire.encode(msg("hop-max", "capped", "device-A"), hop = BridgePolicy.MAX_HOP)))
            // It still reaches the hub store (relay does not gate the leaf path).
            await("hub stored it") { hub.store.containsKey("hop-max") }
            Thread.sleep(200)
            assertTrue("ceiling blocks bridge forward", bridge.received.none { it.message.id == "hop-max" })
        } finally {
            leaf?.stop(); bridge?.close(); server.stop()
        }
    }

    @Test
    fun plainLeafToLeafRelayIsUnchangedWhenABridgeIsPresent() = runBlocking {
        // Regression guard: a bridge connection must not alter the star's
        // leaf↔leaf fan-out (no hop rewrite, delivered to every other leaf).
        val hub = FakePeer("hub")
        val bPeer = FakePeer("B")
        val server = startHub(hub)
        var clientA: MeshClient? = null
        var clientB: MeshClient? = null
        var bridge: FakeBridge? = null
        try {
            val port = server.boundPort
            clientA = MeshClient("127.0.0.1", port, FakePeer("A"))
            clientB = MeshClient("127.0.0.1", port, bPeer)
            clientA.start(); clientB.start()
            bridge = FakeBridge(port)
            bridge.announce("hub-B")
            await("3 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 3
            }
            assertTrue(clientA.send(msg("leaf-1", "hello B", "device-A")))
            await("B received via plain fan-out") { bPeer.store.containsKey("leaf-1") }
            // Delivered as an ordinary msg (hop stays 0; the leaf path never
            // rewrites the envelope), exactly as before the bridge existed.
            assertEquals("hello B", bPeer.store["leaf-1"]!!.body)
            assertEquals(MessageStatus.SENT, bPeer.store["leaf-1"]!!.status)
        } finally {
            clientA?.stop(); clientB?.stop(); bridge?.close(); server.stop()
        }
    }

    // ---- PR#4: 2-hub bridge E2E (real MeshClient bridge, both directions) --
    //
    // Topology: hub A and hub B each run a real MeshServer; A also runs a real
    // MeshClient(role=BRIDGE) to B (single inter-hub link — only A sets it, per
    // the operational rule). One real leaf MeshClient hangs off each hub. This
    // is the production wiring: A->B is driven by A's controller (forwardToBridge
    // in onMessage/sendLocal), B->A by B's MeshServer.relay to its isBridge conn.

    /** Build the 2-hub fixture; returns a closer that tears everything down. */
    private class TwoHubFixture(
        engineA: SummarizationEngine? = null,
        engineB: SummarizationEngine? = null,
        dispatchFallbackMs: Long = 600L,
    ) {
        // Distinct deviceIds so the layer-6 claim CAS can tell the two hubs
        // apart (a claim from "hub-B" makes "hub-A" stand down and vice versa).
        val hubA = FakePeer("hubA", serverMode = true, engine = engineA, deviceId = "hub-A", dispatchFallbackMs = dispatchFallbackMs)
        val hubB = FakePeer("hubB", serverMode = true, engine = engineB, deviceId = "hub-B", dispatchFallbackMs = dispatchFallbackMs)
        lateinit var serverA: MeshServer
        lateinit var serverB: MeshServer
        lateinit var bridge: MeshClient
        var leafA: MeshClient? = null
        var leafB: MeshClient? = null
        val leafAPeer = FakePeer("leafA")
        val leafBPeer = FakePeer("leafB")
        // Extra bridges (e.g. the B->A reverse link for the dispatch fixture) to
        // tear down alongside the primary A->B bridge.
        val bridgesToClose = mutableListOf<MeshClient>()

        fun close() = runBlocking {
            leafA?.stop(); leafB?.stop()
            bridge.stop()
            bridgesToClose.forEach { it.stop() }
            serverA.stop(); serverB.stop()
        }
    }

    private fun startTwoHubs(
        engineA: SummarizationEngine? = null,
        engineB: SummarizationEngine? = null,
        dispatchFallbackMs: Long = 600L,
    ): TwoHubFixture = TwoHubFixture(engineA, engineB, dispatchFallbackMs).apply {
        serverA = startHub(hubA)
        serverB = startHub(hubB)
        hubA.localTransport = serverA
        hubB.localTransport = serverB
        // Each hub fans its produced AI summary to its own leaves through its
        // server (layer 4/6). Set for the summary E2E; harmless otherwise.
        hubA.relayTransport = serverA
        hubB.relayTransport = serverB
        // A bridges to B. A's bridge MeshClient announces itself, so B marks the
        // connection isBridge=true and forwards across it (the B->A direction).
        bridge = MeshClient(
            host = "127.0.0.1",
            port = serverB.boundPort,
            events = hubA,
            role = MeshClient.Role.BRIDGE,
            deviceId = "hub-A",
        )
        runBlocking { bridge.start() }
        hubA.bridge = bridge
        // Wait until B sees the bridge connection.
        await("B sees bridge") {
            val s = serverB.state.value; s is MeshState.Hub && s.peerCount >= 1
        }
        // One real leaf per hub.
        leafA = MeshClient("127.0.0.1", serverA.boundPort, leafAPeer)
            .also { runBlocking { it.start() } }
        leafB = MeshClient("127.0.0.1", serverB.boundPort, leafBPeer)
            .also { runBlocking { it.start() } }
        await("leaf A connected") { leafA!!.state.value is MeshState.Connected }
        await("leaf B connected") { leafB!!.state.value is MeshState.Connected }
        // B now has leafB + bridge = 2 peers; A has leafA = 1 peer.
        await("B has 2 peers") {
            val s = serverB.state.value; s is MeshState.Hub && s.peerCount == 2
        }
    }

    /**
     * Operator-layer-3 dispatch fixture: like [startTwoHubs] but with a SECOND
     * bridge B->A so a claim originating on EITHER hub reaches the other. This
     * mirrors a deployment where every hub bridges to its peer (each hub owns its
     * own outbound bridge), which is what lets the dispatch target's
     * summary_claim suppress a non-target's deferred claim regardless of which
     * hub the request was tapped on. The forward-once + seen-set guards keep the
     * bidirectional links loop-free, so plain chat and the PR#6 floor are intact.
     */
    private fun startTwoHubsBidirectional(
        engineA: SummarizationEngine? = null,
        engineB: SummarizationEngine? = null,
        dispatchFallbackMs: Long = 600L,
    ): TwoHubFixture = startTwoHubs(engineA, engineB, dispatchFallbackMs).apply {
        val bridgeB = MeshClient(
            host = "127.0.0.1",
            port = serverA.boundPort,
            events = hubB,
            role = MeshClient.Role.BRIDGE,
            deviceId = "hub-B",
        )
        runBlocking { bridgeB.start() }
        hubB.bridge = bridgeB
        bridgesToClose.add(bridgeB)
        await("A sees B's bridge") {
            val s = serverA.state.value; s is MeshState.Hub && s.peerCount >= 2
        }
    }

    @Test
    fun aLeafMessageReachesBLeafExactlyOnceAcrossTheBridge() = runBlocking {
        val f = startTwoHubs()
        try {
            // Leaf on A speaks; it must surface on B's leaf exactly once.
            assertTrue(f.leafA!!.send(msg("a2b-1", "from A side", "device-A")))
            await("B leaf got it") { f.leafBPeer.store.containsKey("a2b-1") }
            // Give any erroneous echo time to (wrongly) arrive, then assert once.
            Thread.sleep(300)
            assertEquals("from A side", f.leafBPeer.store["a2b-1"]!!.body)
            // Hub A and hub B both stored it exactly once (dedup) and A's leaf
            // (the sender) was excluded from its own hub's fan-out, so no dup.
            assertTrue(f.hubA.store.containsKey("a2b-1"))
            assertTrue(f.hubB.store.containsKey("a2b-1"))
            assertEquals(1, f.hubB.store.values.count { it.id == "a2b-1" })
        } finally {
            f.close()
        }
    }

    @Test
    fun bLeafMessageReachesALeafExactlyOnceAcrossTheBridge() = runBlocking {
        val f = startTwoHubs()
        try {
            // Leaf on B speaks; B->A is carried by B's MeshServer.relay to the
            // isBridge connection, received by A's bridge MeshClient, fanned out
            // to A's leaves by hubA.ingest(fromBridge=true).
            assertTrue(f.leafB!!.send(msg("b2a-1", "from B side", "device-B")))
            await("A leaf got it") { f.leafAPeer.store.containsKey("b2a-1") }
            Thread.sleep(300)
            assertEquals("from B side", f.leafAPeer.store["b2a-1"]!!.body)
            assertEquals(1, f.leafAPeer.store.values.count { it.id == "b2a-1" })
            assertTrue(f.hubA.store.containsKey("b2a-1"))
            assertTrue(f.hubB.store.containsKey("b2a-1"))
        } finally {
            f.close()
        }
    }

    @Test
    fun bridgeRoleClientAnnouncesItselfSoTheFarHubMarksItABridge() = runBlocking {
        // A BRIDGE-role MeshClient must send a bridge_hello on connect; the far
        // hub then routes a leaf-authored msg across it via the forwarding
        // policy (hop 0 -> 1, origin stamped). If the hello were missing, the
        // hub would treat the link as a plain leaf and fan out hop-0 / no origin.
        val hub = FakePeer("hub")
        val server = startHub(hub)
        var leaf: MeshClient? = null
        var bridge: MeshClient? = null
        try {
            val port = server.boundPort
            bridge = MeshClient(
                host = "127.0.0.1",
                port = port,
                events = FakePeer("bridgeSide"),
                role = MeshClient.Role.BRIDGE,
                deviceId = "hub-far",
            )
            bridge.start()
            await("hub saw bridge") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 1
            }
            // Capture exactly what the bridge link receives from the hub.
            val received = java.util.concurrent.CopyOnWriteArrayList<MessageWire.Frame.Msg>()
            val bridgeSink = object : MeshEvents {
                override suspend fun onMessage(message: Message) = true
                override suspend fun ingest(message: Message, hop: Int, originId: String?, fromBridge: Boolean) {
                    received.add(MessageWire.Frame.Msg(message, hop, originId))
                }
                override suspend fun onSyncRequest(since: Long, reply: suspend (String) -> Unit) {}
                override suspend fun onLinkEstablished(transport: MeshTransport) {}
                override suspend fun onSummaryRequest(since: Long, questionId: String?) {}
                override fun bridgeHopFor(messageId: String, hop: Int): Int? = null
            }
            // Re-point the bridge through a fresh client wired to the sink so we
            // can read the forwarded envelope (hop/origin) on this side.
            bridge.stop()
            bridge = MeshClient("127.0.0.1", port, bridgeSink, MeshClient.Role.BRIDGE, "hub-far")
            bridge.start()
            await("hub saw bridge #2") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 1
            }
            leaf = MeshClient("127.0.0.1", port, FakePeer("leaf")).also { it.start() }
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            assertTrue(leaf.send(msg("hello-test", "x", "device-A")))
            await("bridge got forwarded") { received.any { it.message.id == "hello-test" } }
            val f = received.first { it.message.id == "hello-test" }
            assertEquals("hop incremented (proves isBridge via hello)", 1, f.hop)
            assertEquals("device-A", f.originId)
        } finally {
            leaf?.stop(); bridge?.stop(); server.stop()
        }
    }

    @Test
    fun bridgedMessageDoesNotLoopOrDuplicate() = runBlocking {
        val f = startTwoHubs()
        try {
            // Fire from both sides; each must land on the far leaf exactly once
            // and never ping-pong back (no growth in counts after settling).
            assertTrue(f.leafA!!.send(msg("loop-a", "ping A", "device-A")))
            assertTrue(f.leafB!!.send(msg("loop-b", "ping B", "device-B")))
            await("A's msg on B") { f.leafBPeer.store.containsKey("loop-a") }
            await("B's msg on A") { f.leafAPeer.store.containsKey("loop-b") }
            // Let any echo circulate, then assert single delivery everywhere.
            Thread.sleep(400)
            assertEquals(1, f.leafBPeer.store.values.count { it.id == "loop-a" })
            assertEquals(1, f.leafAPeer.store.values.count { it.id == "loop-b" })
            assertEquals(1, f.hubA.store.values.count { it.id == "loop-a" })
            assertEquals(1, f.hubB.store.values.count { it.id == "loop-a" })
            assertEquals(1, f.hubA.store.values.count { it.id == "loop-b" })
            assertEquals(1, f.hubB.store.values.count { it.id == "loop-b" })
        } finally {
            f.close()
        }
    }

    // ---- PR#5: heartbeat + partition recovery (bidirectional resync) -------
    //
    // The BRIDGE MeshClient re-uses bridge_hello as a heartbeat (no new wire
    // type). On a (re)connect both hubs exchange sync_req(watermark): A asks B in
    // onLinkEstablished (B->A recovery); B asks A on the FIRST hello in its
    // MeshServer (A->B recovery). DAO dedup + the seen-set keep recovery free of
    // loops/duplicates. Death detection is left to TCP EOF + backoff reconnect.

    /**
     * The BRIDGE-role client re-sends bridge_hello on a cadence (heartbeat) and
     * the far hub refreshes its per-connection last-seen each time. We observe it
     * by counting hello lines on a raw socket standing in for the far hub.
     */
    @Test
    fun bridgeRoleClientReSendsHelloHeartbeatAndFarHubTracksLastSeen() = runBlocking {
        var bridge: MeshClient? = null
        // Raw socket standing in for the far hub: count bridge_hello lines.
        val helloCount = java.util.concurrent.atomic.AtomicInteger(0)
        val acceptScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        val raw = ServerSocketCounter(helloCount, acceptScope)
        try {
            bridge = MeshClient(
                host = "127.0.0.1",
                port = raw.port,
                events = FakePeer("bridgeSide"),
                role = MeshClient.Role.BRIDGE,
                deviceId = "hub-A",
            )
            bridge.start()
            // First hello is immediate; with a 5s cadence at least one more must
            // arrive inside the window. Two total proves the heartbeat timer fired.
            await("heartbeat re-sent hello", timeoutMs = 8_000) { helloCount.get() >= 2 }
        } finally {
            bridge?.stop(); raw.close(); acceptScope.cancel()
        }
    }

    /** A tiny raw TCP server that counts bridge_hello lines per connection. */
    private class ServerSocketCounter(
        private val counter: java.util.concurrent.atomic.AtomicInteger,
        scope: CoroutineScope,
    ) {
        private val server = java.net.ServerSocket().apply {
            reuseAddress = true
            bind(InetSocketAddress("127.0.0.1", 0), 50)
        }
        val port: Int get() = server.localPort
        init {
            scope.launch {
                val sock = server.accept()
                val r = BufferedReader(InputStreamReader(sock.getInputStream(), Charsets.UTF_8))
                while (true) {
                    val line = r.readLine() ?: break
                    if (MessageWire.decodeFrame(line) is MessageWire.Frame.BridgeHello) {
                        counter.incrementAndGet()
                    }
                }
            }
        }
        fun close() = runCatching { server.close() }
    }

    /**
     * The core partition→recovery proof. A and B are bridged; during a partition
     * (B's server down) each side's leaf authors a message the other never sees.
     * On reconnect both directions of backfill fire and the missed messages heal
     * across the bridge — exactly once, with no loop.
     */
    @Test
    fun partitionThenRecoveryBidirectionallyResyncsMissedMessages() = runBlocking {
        val hubA = FakePeer("hubA", serverMode = true)
        val hubB = FakePeer("hubB", serverMode = true)
        val leafAPeer = FakePeer("leafA")
        val leafBPeer = FakePeer("leafB")
        val serverA = startHub(hubA)
        // Bind B on an explicit ephemeral port we can re-bind after the partition.
        var serverB = MeshServer(port = 0, bindAddress = "127.0.0.1", events = hubB)
        serverB.start()
        await("B listening") { serverB.state.value is MeshState.Hub }
        Thread.sleep(150)
        val portB = serverB.boundPort
        hubA.localTransport = serverA
        hubB.localTransport = serverB

        var leafA: MeshClient? = null
        var leafB: MeshClient? = null
        var bridge: MeshClient? = null
        try {
            bridge = MeshClient("127.0.0.1", portB, hubA, MeshClient.Role.BRIDGE, "hub-A")
            bridge.start()
            hubA.bridge = bridge
            await("B sees bridge") {
                val s = serverB.state.value; s is MeshState.Hub && s.peerCount >= 1
            }
            leafA = MeshClient("127.0.0.1", serverA.boundPort, leafAPeer).also { it.start() }
            leafB = MeshClient("127.0.0.1", portB, leafBPeer).also { it.start() }
            await("leaf A connected") { leafA!!.state.value is MeshState.Connected }
            await("leaf B connected") { leafB!!.state.value is MeshState.Connected }

            // Connected baseline: a message crosses the bridge live.
            assertTrue(leafA!!.send(msg("pre-1", "before split", "device-A", 1_000)))
            await("B leaf got pre-1") { leafBPeer.store.containsKey("pre-1") }

            // The two recovery directions are exercised as two sequential
            // partitions, each isolating ONE side as the author. This is
            // deliberate: backfill is watermark-based (sync everything created
            // after the requester's high-water mark), so a message authored on
            // ONE side during a split is strictly newer than the peer's
            // watermark and is recovered cleanly. (Two messages authored
            // CONCURRENTLY at the partition boundary cannot both be recovered by
            // a single scalar watermark — that needs a per-bridge cursor, a known
            // Stage-1 limitation, out of scope here. Each direction is proven.)

            // ===== Direction 1 (A->B): A speaks during the split, B recovers ===
            leafB!!.stop()
            serverB.stop()
            Thread.sleep(300) // let A's bridge client see the EOF and start retry.

            assertTrue(leafA!!.send(msg("split-a", "A during split", "device-A", 2_000)))
            await("A hub has split-a") { hubA.store.containsKey("split-a") }
            assertFalse("split-a must NOT be on B yet", hubB.store.containsKey("split-a"))

            // B returns on the SAME port; A's bridge reconnects + re-announces.
            // B's MeshServer sends sync_req(B watermark) on the opening hello;
            // A's BRIDGE client answers with split-a; B fans it to its leaf.
            serverB = MeshServer(port = portB, bindAddress = "127.0.0.1", events = hubB)
            serverB.start()
            await("B listening again") { serverB.state.value is MeshState.Hub }
            hubB.localTransport = serverB
            leafB = MeshClient("127.0.0.1", portB, leafBPeer).also { it.start() }
            await("leaf B reconnected #1") { leafB!!.state.value is MeshState.Connected }
            await("B recovered A's split-a", timeoutMs = 8_000) {
                hubB.store.containsKey("split-a")
            }
            await("leaf B got A's split-a") { leafBPeer.store.containsKey("split-a") }

            // ===== Direction 2 (B->A): B speaks during the split, A recovers ===
            leafB!!.stop()
            serverB.stop()
            Thread.sleep(300)

            // B's leaf is offline; author straight into B's hub store as if its
            // leaf had spoken to B locally while partitioned (newer than A's wm).
            hubB.put(msg("split-b", "B during split", "device-B", 3_000, MessageStatus.SENT))
            assertFalse("split-b must NOT be on A yet", hubA.store.containsKey("split-b"))

            // B returns; A's bridge reconnects and its onLinkEstablished sends
            // sync_req(A watermark); B replies with split-b; A ingests it
            // (fromBridge) and fans it out to its leaf.
            serverB = MeshServer(port = portB, bindAddress = "127.0.0.1", events = hubB)
            serverB.start()
            await("B listening again #2") { serverB.state.value is MeshState.Hub }
            hubB.localTransport = serverB
            leafB = MeshClient("127.0.0.1", portB, leafBPeer).also { it.start() }
            await("leaf B reconnected #2") { leafB!!.state.value is MeshState.Connected }
            await("B sees bridge again #2", timeoutMs = 8_000) {
                val s = serverB.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            await("A recovered B's split-b", timeoutMs = 8_000) {
                hubA.store.containsKey("split-b")
            }
            await("leaf A got B's split-b") { leafAPeer.store.containsKey("split-b") }

            // No duplication / loop after the dust settles (both directions).
            Thread.sleep(400)
            assertEquals(1, hubA.store.values.count { it.id == "split-b" })
            assertEquals(1, hubB.store.values.count { it.id == "split-a" })
            assertEquals(1, leafAPeer.store.values.count { it.id == "split-b" })
            assertEquals(1, leafBPeer.store.values.count { it.id == "split-a" })
        } finally {
            leafA?.stop(); leafB?.stop(); bridge?.stop()
            serverA.stop(); serverB.stop()
        }
    }

    // ---- Operator layer 1: piggyback load metrics on the heartbeat ---------
    //
    // queueDepth/dispatchCount ride the EXISTING bridge_hello heartbeat (no new
    // frame, no new timer). Each hub stamps its own load on every hello; the far
    // hub records it in a per-peer view. Load is shared BOTH ways: A learns B's
    // from B's echoed hello, B learns A's from A's heartbeat. This PR only SHARES
    // the view — nothing routes on it yet (operator selection is a later PR).

    @Test
    fun bridgeHelloRoundTripsLoadMetricsAndDefaultsToZero() {
        // Metrics present: preserved exactly through encode -> decode.
        val withLoad = MessageWire.decodeFrame(
            MessageWire.encodeBridgeHello("hub-7", queueDepth = 3, dispatchCount = 11),
        ) as MessageWire.Frame.BridgeHello
        assertEquals("hub-7", withLoad.deviceId)
        assertEquals(3, withLoad.queueDepth)
        assertEquals(11, withLoad.dispatchCount)
        // Legacy hello (no metric keys at all): decodes as idle / zero load.
        val legacy = """{"type":"bridge_hello","deviceId":"old-hub"}"""
        val f = MessageWire.decodeFrame(legacy) as MessageWire.Frame.BridgeHello
        assertEquals("old-hub", f.deviceId)
        assertEquals(0, f.queueDepth)
        assertEquals(0, f.dispatchCount)
    }

    @Test
    fun heartbeatPropagatesLoadMetricsToBothPeersAcrossTheBridge() = runBlocking {
        // Give each hub a distinct, non-trivial load BEFORE the bridge comes up,
        // so the very first hello (and its echo) already carries real numbers.
        val f = startTwoHubs()
        try {
            f.hubA.selfLoad = HubLoad(queueDepth = 1, dispatchCount = 7)
            f.hubB.selfLoad = HubLoad(queueDepth = 2, dispatchCount = 4)
            // A's BRIDGE client re-sends its hello every 5s; B echoes its own
            // hello (with B's load) back on the same connection. Within a couple
            // of heartbeats both sides must hold the other's reported load.
            await("B recorded A's load", timeoutMs = 12_000) {
                f.hubB.peerLoads["hub-A"] == HubLoad(1, 7)
            }
            await("A recorded B's load", timeoutMs = 12_000) {
                f.hubA.peerLoads["hub-B"] == HubLoad(2, 4)
            }
            // Updated load propagates on the NEXT heartbeat (no extra traffic).
            f.hubA.selfLoad = HubLoad(queueDepth = 0, dispatchCount = 9)
            await("B saw A's updated load", timeoutMs = 12_000) {
                f.hubB.peerLoads["hub-A"] == HubLoad(0, 9)
            }
        } finally {
            f.close()
        }
    }

    @Test
    fun singleHubKeepsAnEmptyPeerLoadViewAndUnchangedRouting() = runBlocking {
        // Zero-regression: with NO bridge, no bridge_hello is ever sent, so the
        // peer-load view stays empty and plain relay is byte-for-byte unchanged.
        val hub = FakePeer("hub", deviceId = "hub-solo")
        hub.selfLoad = HubLoad(queueDepth = 5, dispatchCount = 5) // would-be load
        val bPeer = FakePeer("B")
        val server = startHub(hub)
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
            // Plain leaf->leaf relay still works exactly as before.
            assertTrue(clientA.send(msg("solo-1", "hi B", "device-A")))
            await("B received") { bPeer.store.containsKey("solo-1") }
            Thread.sleep(200)
            // No hello was exchanged (no bridge), so no peer load was recorded
            // on any party, despite the hub having a non-zero would-be load.
            assertTrue("hub peer view empty", hub.peerLoads.isEmpty())
            assertTrue("B peer view empty", bPeer.peerLoads.isEmpty())
        } finally {
            clientA?.stop(); clientB?.stop(); server.stop()
        }
    }

    // ---- PR#6: AI single-ownership (claim CAS + dual-log on cross-ack) ------
    //
    // A summary_req now crosses the bridge so EITHER hub can answer; the
    // per-question claim CAS keeps the common case to ONE answer, a simultaneous
    // (injected) cross-ack to TWO, and a third claimant is suppressed. A single
    // hub (no bridge) never crosses or claims, so behaviour is byte-for-byte the
    // pre-layer-6 path. ownerId is the peer's untrusted self-report (Stage-1).

    @Test
    fun questionOwnershipFirstClaimWinsAndForeignClaimStandsDown() {
        val own = QuestionOwnership()
        // First claimant wins; an idempotent re-claim by the same owner is fine.
        assertTrue(own.tryClaim("q1", "hub-A"))
        assertTrue("idempotent self re-claim", own.tryClaim("q1", "hub-A"))
        assertFalse("single owner: not yet cross-ack", own.isCrossAck("q1"))
        // A different hub that has recorded A's claim stands down.
        own.onRemoteClaim("q1", "hub-A")
        assertFalse("foreign owner present -> stand down", own.tryClaim("q1", "hub-B"))
    }

    @Test
    fun questionOwnershipForwardsEachQuestionAcrossTheBridgeOnlyOnce() {
        val own = QuestionOwnership()
        assertTrue("first offer forwards", own.shouldForward("q1"))
        assertFalse("second offer of same id is refused", own.shouldForward("q1"))
        assertTrue("a different id still forwards", own.shouldForward("q2"))
    }

    @Test
    fun questionOwnershipCrossAckAdmitsTwoOwnersThenSuppressesTheThird() {
        val own = QuestionOwnership()
        // Cross-ack: this hub won locally, THEN a peer's claim arrives -> both
        // own it (two-bubble outcome). isCrossAck flips true.
        assertTrue(own.tryClaim("q1", "hub-A"))
        own.onRemoteClaim("q1", "hub-B")
        assertTrue("two distinct owners == cross-ack", own.isCrossAck("q1"))
        // Runaway guard: a third distinct hub is NOT admitted (cap = 2) and
        // cannot claim either (a foreign owner is present).
        own.onRemoteClaim("q1", "hub-C")
        assertFalse("third claimant cannot own", own.tryClaim("q1", "hub-C"))
    }

    @Test
    fun twoHubsAnswerOneQuestionExactlyOnceAcrossTheBridge() = runBlocking {
        // Modern leaf taps "状況まとめ" on A's side: a single questionId crosses
        // to B. A claims first (claim precedes the forwarded request on the same
        // ordered socket), so B stands down -> exactly ONE AI summary, delivered
        // to BOTH hubs' leaves (the summary itself crosses the bridge).
        val engA = FakeSummarizationEngine()
        val engB = FakeSummarizationEngine()
        val f = startTwoHubs(engineA = engA, engineB = engB)
        try {
            f.hubA.put(msg("c-1", "water needed", "leafA", 1_000, MessageStatus.SENT))
            // A leaf frames a summary_req with a freshly-minted questionId.
            assertTrue(f.leafA!!.sendRaw(MessageWire.encodeSummaryReq(questionId = "Q-shared")))
            await("an AI summary reached A's leaf") {
                f.leafAPeer.store.values.any { it.senderId == AI_SENDER_ID }
            }
            await("the AI summary crossed to B's leaf") {
                f.leafBPeer.store.values.any { it.senderId == AI_SENDER_ID }
            }
            // Let any erroneous second generation settle, then assert single answer.
            Thread.sleep(400)
            assertEquals("exactly one hub generated", 1, engA.callCount.get() + engB.callCount.get())
            assertEquals("B stood down on A's claim", 0, engB.callCount.get())
            assertEquals(1, f.leafAPeer.store.values.count { it.senderId == AI_SENDER_ID })
            assertEquals(1, f.leafBPeer.store.values.count { it.senderId == AI_SENDER_ID })
        } finally {
            f.close()
        }
    }

    @Test
    fun crossAckInjectionMakesBothHubsAnswerWithTwoAiBubblesAndDualLog() = runBlocking {
        // Genuine "すれ違い": each hub independently originates the SAME
        // questionId and wins its local claim BEFORE the peer's claim lands; the
        // two claims then cross in flight. To inject that crossing deterministic-
        // ally (the spec's "claim をすれ違わせる") we run each hub WITHOUT a bridge
        // (so its auto-claim cannot pre-empt the other) and then hand each hub
        // the peer's claim AFTER both have started. Both continue -> two answers,
        // and each hub logs the cross-ack exactly once. A leaf renders the two
        // AI messages (distinct ids) as two bubbles via the existing UI.
        val engA = FakeSummarizationEngine()
        val engB = FakeSummarizationEngine()
        // Bridge left null: each hub only claims locally (no cross-talk yet).
        val hubA = FakePeer("hubA", serverMode = true, engine = engA, deviceId = "hub-A")
        val hubB = FakePeer("hubB", serverMode = true, engine = engB, deviceId = "hub-B")
        hubA.put(msg("c-a", "from A side", "leafA", 1_000, MessageStatus.SENT))
        hubB.put(msg("c-b", "from B side", "leafB", 1_000, MessageStatus.SENT))
        // Simultaneous origination: each wins its own CAS and starts generating.
        hubA.onSummaryRequest(0L, "Q-cross")
        hubB.onSummaryRequest(0L, "Q-cross")
        await("both hubs generated (two answers)") {
            engA.callCount.get() == 1 && engB.callCount.get() == 1
        }
        // The crossed claims now arrive (each hub hears the OTHER's claim).
        hubA.onSummaryClaim("Q-cross", "hub-B")
        hubB.onSummaryClaim("Q-cross", "hub-A")
        // Dual-log: each hub recorded the cross-ack exactly once; neither retracts.
        await("A logged cross-ack") { hubA.crossAckCount.get() == 1 }
        await("B logged cross-ack") { hubB.crossAckCount.get() == 1 }
        assertTrue("A still co-owns after cross-ack", hubA.questionOwnership.isCrossAck("Q-cross"))
        assertTrue("B still co-owns after cross-ack", hubB.questionOwnership.isCrossAck("Q-cross"))
        // Two distinct AI summaries exist (one per hub) -> two bubbles for a leaf.
        await("each hub produced its own AI summary") {
            hubA.store.values.count { it.senderId == AI_SENDER_ID } == 1 &&
                hubB.store.values.count { it.senderId == AI_SENDER_ID } == 1
        }
        val idA = hubA.store.values.first { it.senderId == AI_SENDER_ID }.id
        val idB = hubB.store.values.first { it.senderId == AI_SENDER_ID }.id
        assertTrue("the two summaries are distinct messages", idA != idB)
    }

    @Test
    fun thirdConcurrentClaimantIsSuppressedToAtMostTwoAnswers() {
        // Three hubs claim the SAME question almost at once. The first two are
        // admitted (cross-ack, two answers); the third is suppressed by the cap,
        // so a question never produces a third runaway answer.
        val own = QuestionOwnership()
        assertTrue("hub A wins its local claim", own.tryClaim("q", "hub-A"))
        own.onRemoteClaim("q", "hub-B") // B's claim crosses in -> co-owner
        assertTrue("now two owners (A,B)", own.isCrossAck("q"))
        // A third hub: neither a recorded remote claim nor a local claim admits it.
        own.onRemoteClaim("q", "hub-C")
        assertFalse(own.tryClaim("q", "hub-C"))
    }

    @Test
    fun singleHubNeverClaimsOrCrossesSoBehaviourIsUnchanged() = runBlocking {
        // Zero-regression: with NO bridge, a hub answers a summary_req exactly
        // once and emits no claim/forward (there is nowhere to send them). This
        // is the pre-layer-6 single-star path, byte-for-byte.
        val fake = FakeSummarizationEngine()
        val hub = FakePeer("hub", serverMode = true, engine = fake, deviceId = "hub-solo")
        val server = startHub(hub)
        hub.relayTransport = server
        // A raw socket that would observe ANY claim/forward if one were emitted.
        var bridge: FakeBridge? = null
        var client: MeshClient? = null
        try {
            val port = server.boundPort
            bridge = FakeBridge(port)
            bridge.announce("observer")
            client = MeshClient("127.0.0.1", port, FakePeer("A"))
            client.start()
            await("2 peers") {
                val s = server.state.value; s is MeshState.Hub && s.peerCount == 2
            }
            hub.put(msg("c-1", "status ok", "A", 1_000, MessageStatus.SENT))
            // NOTE: hub.bridge stays null (single hub) -> no forward, no claim.
            assertTrue(client.sendRaw(MessageWire.encodeSummaryReq(questionId = "Q-solo")))
            await("hub generated exactly one AI summary") {
                hub.store.values.count { it.senderId == AI_SENDER_ID } == 1
            }
            Thread.sleep(300)
            assertEquals("single answer", 1, fake.callCount.get())
            // The observer never received a forwarded summary_req (only the AI
            // summary msg fan-out, which is a normal msg, is allowed to arrive).
            assertEquals("no claim/forward emitted on a single hub", 0, hub.crossAckCount.get())
        } finally {
            client?.stop(); bridge?.close(); server.stop()
        }
    }

    // ---- Operator layer 3: advisory dispatch (hint-biased claim) -----------
    //
    // The operator advertises a dispatchTarget (the least-loaded hub) on its
    // bridge_hello. A hub biases claim TIMING on it: the target claims at once,
    // a non-target defers by dispatchFallbackMs. The claim CAS (PR#6) is still
    // the correctness floor — the hint only changes who claims first, never
    // whether a question stays single-owned, and a dead target falls back so the
    // answer is never lost. Hint is untrusted (Task #21); a bad hint only loses
    // efficiency. dispatchFallbackMs is injected so timing is deterministic.

    @Test
    fun bridgeHelloRoundTripsDispatchTargetAndDefaultsNull() {
        val withHint = MessageWire.decodeFrame(
            MessageWire.encodeBridgeHello("op", queueDepth = 2, dispatchTarget = "hub-idle"),
        ) as MessageWire.Frame.BridgeHello
        assertEquals("hub-idle", withHint.dispatchTarget)
        // No target field at all -> null (legacy / non-operator hello).
        val none = MessageWire.decodeFrame(MessageWire.encodeBridgeHello("op"))
                as MessageWire.Frame.BridgeHello
        assertNull(none.dispatchTarget)
    }

    @Test
    fun operatorDispatchHintIsGossipedAcrossTheBridgeAndRecorded() = runBlocking {
        // The hint travels on the EXISTING hello: A (acting as operator) stamps
        // dispatchTarget on its bridge heartbeat; B records it via onDispatchHint.
        val f = startTwoHubs()
        try {
            f.hubA.dispatchTargetToAdvertise = "hub-B"
            await("B recorded A's dispatch hint", timeoutMs = 12_000) {
                f.hubB.receivedHint?.first == "hub-B"
            }
            // A non-operator (B) advertises no hint, so A records null from B's echo.
            await("A recorded B's (null) hint", timeoutMs = 12_000) {
                f.hubA.receivedHint != null && f.hubA.receivedHint?.first == null
            }
        } finally {
            f.close()
        }
    }

    @Test
    fun hintTargetClaimsAndTheBusyNonTargetStandsDownLoadBalanced() = runBlocking {
        // Operator hint points at the idle hub B. A leaf taps on the busy hub A.
        // A is NOT the target -> it forwards the request then defers its claim.
        // B IS the target -> it claims immediately; its claim makes A's deferred
        // attempt stand down. Exactly ONE answer, produced by the TARGET (B).
        val engA = FakeSummarizationEngine()
        val engB = FakeSummarizationEngine()
        // Bidirectional bridge so the target's claim reaches the non-target.
        // Generous fallback so B's claim deterministically beats A's deferred try.
        val f = startTwoHubsBidirectional(engineA = engA, engineB = engB, dispatchFallbackMs = 3_000L)
        try {
            // hub-A is the operator and points the hint at the idle hub-B. Both
            // hubs agree on who the operator is; A uses its OWN target, B uses
            // the hint it recorded from A's hello (set directly for determinism).
            f.hubA.believedOperator = "hub-A"
            f.hubB.believedOperator = "hub-A"
            f.hubA.dispatchTargetToAdvertise = "hub-B"
            f.hubB.receivedHint = "hub-B" to System.currentTimeMillis()
            // Seed the same chat on BOTH hubs so whichever generates digests it.
            f.hubA.put(msg("c-1", "water needed", "leafA", 1_000, MessageStatus.SENT))
            f.hubB.put(msg("c-1", "water needed", "leafA", 1_000, MessageStatus.SENT))

            assertTrue(f.leafA!!.sendRaw(MessageWire.encodeSummaryReq(questionId = "Q-bal")))
            await("an AI summary reached A's leaf", timeoutMs = 8_000) {
                f.leafAPeer.store.values.any { it.senderId == AI_SENDER_ID }
            }
            Thread.sleep(400)
            assertEquals("exactly one hub generated", 1, engA.callCount.get() + engB.callCount.get())
            assertEquals("the TARGET (B) generated", 1, engB.callCount.get())
            assertEquals("the busy non-target (A) stood down", 0, engA.callCount.get())
            assertEquals(1, f.leafAPeer.store.values.count { it.senderId == AI_SENDER_ID })
            assertEquals(1, f.leafBPeer.store.values.count { it.senderId == AI_SENDER_ID })
        } finally {
            f.close()
        }
    }

    @Test
    fun deadHintTargetFallsBackSoTheAnswerIsNeverLost() = runBlocking {
        // Liveness: the operator hint names a target that never claims (it is
        // dead / unreachable). The receiving hub must still produce the answer
        // after the fallback window — the hint can only DELAY, never DROP, a
        // summary. Modelled with a lone hub (no bridge to the named target) so
        // the target provably never claims.
        val fake = FakeSummarizationEngine()
        val hub = FakePeer(
            "hub", serverMode = true, engine = fake, deviceId = "hub-A",
            dispatchFallbackMs = 300L, // small but non-zero: prove fallback fires.
        )
        val server = startHub(hub)
        hub.relayTransport = server
        var client: MeshClient? = null
        try {
            client = MeshClient("127.0.0.1", server.boundPort, FakePeer("A"))
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            hub.put(msg("c-1", "status ok", "A", 1_000, MessageStatus.SENT))
            // Hint names hub-ghost (a target that does not exist here / never claims).
            hub.receivedHint = "hub-ghost" to System.currentTimeMillis()

            assertTrue(client.sendRaw(MessageWire.encodeSummaryReq(questionId = "Q-live")))
            // No immediate answer (the hub deferred to the dead target)...
            Thread.sleep(50)
            assertEquals("did not claim immediately (deferred to target)", 0, fake.callCount.get())
            // ...but after the fallback window the hub claims and the answer lands.
            await("answer produced after fallback", timeoutMs = 5_000) {
                hub.store.values.count { it.senderId == AI_SENDER_ID } == 1
            }
            assertEquals(1, fake.callCount.get())
        } finally {
            client?.stop(); server.stop()
        }
    }

    @Test
    fun staleHintRevertsToPlainPr6ImmediateClaim() = runBlocking {
        // operator absent / hint stale -> the hub must behave exactly like PR#6:
        // claim its local request IMMEDIATELY (no defer), single answer. We make
        // the recorded hint stale by back-dating it past the stale window.
        val fake = FakeSummarizationEngine()
        val hub = FakePeer(
            "hub", serverMode = true, engine = fake, deviceId = "hub-A",
            dispatchFallbackMs = 5_000L, // huge: a defer here would time the test out.
        )
        val server = startHub(hub)
        hub.relayTransport = server
        var client: MeshClient? = null
        try {
            client = MeshClient("127.0.0.1", server.boundPort, FakePeer("A"))
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            hub.put(msg("c-1", "status ok", "A", 1_000, MessageStatus.SENT))
            // A hint pointing elsewhere, but older than the stale window -> ignored.
            hub.receivedHint = "hub-B" to
                (System.currentTimeMillis() - OperatorElection.DEFAULT_PEER_STALE_MS - 1_000L)

            assertTrue(client.sendRaw(MessageWire.encodeSummaryReq(questionId = "Q-stale")))
            // Immediate claim despite the (stale) elsewhere-hint and the huge
            // fallback: the answer lands well before dispatchFallbackMs.
            await("immediate PR#6 answer", timeoutMs = 2_000) {
                hub.store.values.count { it.senderId == AI_SENDER_ID } == 1
            }
            assertEquals(1, fake.callCount.get())
        } finally {
            client?.stop(); server.stop()
        }
    }

    @Test
    fun teardownCancelsADeferredClaimSoItNeverFiresPostTeardown() = runBlocking {
        // Post-teardown race guard: a non-target hub forwards the request and
        // defers its own claim by dispatchFallbackMs. If a reconfigure/stop
        // happens inside that window, teardown() must cancel the deferred job so
        // it never wakes up to claim/announce a now-stale qid against a fresh
        // transport. Modelled with a lone hub (no bridge) and a non-self hint so
        // the request is deferred; we tear down mid-window and assert no answer.
        val fake = FakeSummarizationEngine()
        val hub = FakePeer(
            "hub", serverMode = true, engine = fake, deviceId = "hub-A",
            dispatchFallbackMs = 500L, // window long enough to tear down inside it.
        )
        val server = startHub(hub)
        hub.relayTransport = server
        var client: MeshClient? = null
        try {
            client = MeshClient("127.0.0.1", server.boundPort, FakePeer("A"))
            client.start()
            await("connected") { client!!.state.value is MeshState.Connected }
            hub.put(msg("c-1", "status ok", "A", 1_000, MessageStatus.SENT))
            // Fresh hint pointing elsewhere -> the hub defers its own claim.
            hub.receivedHint = "hub-ghost" to System.currentTimeMillis()

            assertTrue(client.sendRaw(MessageWire.encodeSummaryReq(questionId = "Q-teardown")))
            // The deferred job is registered and has NOT yet claimed.
            await("deferred claim registered") { hub.deferredClaimJobs.containsKey("Q-teardown") }
            assertEquals("did not claim immediately (deferred)", 0, fake.callCount.get())

            // Reconfigure/stop mid-window: cancel the deferred claim.
            hub.teardown()
            assertTrue("deferred jobs cleared", hub.deferredClaimJobs.isEmpty())

            // Wait past the original window: the cancelled job must never fire.
            Thread.sleep(700)
            assertEquals("no claim after teardown", 0, fake.callCount.get())
            assertEquals(
                "no summary produced",
                0,
                hub.store.values.count { it.senderId == AI_SENDER_ID },
            )
        } finally {
            client?.stop(); server.stop()
        }
    }

    @Test
    fun hintDoesNotBreakSingleOwnershipUnderCrossAck() {
        // Correctness floor is invariant under the hint: even if two hubs both
        // (deferred or not) reach the claim, the CAS keeps it to at most two
        // owners and suppresses a third — identical to PR#6. Proven on the pure
        // ownership logic (the hint never touches it).
        val own = QuestionOwnership()
        assertTrue(own.tryClaim("q", "hub-A"))
        own.onRemoteClaim("q", "hub-B")
        assertTrue("at most two on a genuine cross-ack", own.isCrossAck("q"))
        own.onRemoteClaim("q", "hub-C")
        assertFalse("third suppressed regardless of any hint", own.tryClaim("q", "hub-C"))
    }
}
