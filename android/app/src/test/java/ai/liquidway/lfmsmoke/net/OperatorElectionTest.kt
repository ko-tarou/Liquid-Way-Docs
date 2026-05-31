package ai.liquidway.lfmsmoke.net

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

/**
 * Unit proof of the operator-layer-2 election ([OperatorElection]).
 *
 * Pure logic, no transport: this PR *computes* the operator and exposes it but
 * does NOT act on it (routing/ownership unchanged — that is PR#9). These tests
 * are therefore the authoritative proof of the four properties the design rests
 * on: determinism, cross-hub agreement, flapping protection (hysteresis),
 * silent-peer age-out, and clamping of untrusted self-reported metrics.
 */
class OperatorElectionTest {

    private val now = 1_000_000L

    private fun peer(load: HubLoad, ageMs: Long = 0L) =
        MeshController.PeerLoad(load, seenAt = now - ageMs)

    // ---- Determinism -----------------------------------------------------

    @Test
    fun highestQueueDepthWins() {
        val e = OperatorElection()
        val op = e.elect(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = 0),
            peers = mapOf("hub-B" to peer(HubLoad(queueDepth = 5))),
            now = now,
        )
        assertEquals("busiest hub (B) is the operator", "hub-B", op)
    }

    @Test
    fun tieIsBrokenByLexicographicallySmallestDeviceId() {
        val e = OperatorElection()
        val op = e.elect(
            selfId = "hub-Z",
            selfLoad = HubLoad(queueDepth = 3),
            peers = mapOf(
                "hub-A" to peer(HubLoad(queueDepth = 3)),
                "hub-M" to peer(HubLoad(queueDepth = 3)),
            ),
            now = now,
        )
        assertEquals("all tied -> smallest id wins", "hub-A", op)
    }

    @Test
    fun sameInputAlwaysProducesSameOperator() {
        val peers = mapOf(
            "hub-B" to peer(HubLoad(queueDepth = 2)),
            "hub-C" to peer(HubLoad(queueDepth = 4)),
        )
        // Two independent instances, same input -> identical output (pure fn).
        val first = OperatorElection().elect("hub-A", HubLoad(0), peers, now)
        val second = OperatorElection().elect("hub-A", HubLoad(0), peers, now)
        assertEquals("hub-C", first)
        assertEquals(first, second)
    }

    // ---- Cross-hub agreement (the split-brain-free property) -------------

    @Test
    fun allHubsComputeTheSameOperatorFromTheSamePicture() {
        // The SAME global load picture, viewed from each of three hubs (each hub
        // sees itself via selfLoad and the other two via peers). Loads: A=1, B=4,
        // C=4 -> tie between B and C broken to "hub-B". Every hub must agree.
        val la = HubLoad(queueDepth = 1)
        val lb = HubLoad(queueDepth = 4)
        val lc = HubLoad(queueDepth = 4)

        val fromA = OperatorElection().elect(
            "hub-A", la,
            mapOf("hub-B" to peer(lb), "hub-C" to peer(lc)), now,
        )
        val fromB = OperatorElection().elect(
            "hub-B", lb,
            mapOf("hub-A" to peer(la), "hub-C" to peer(lc)), now,
        )
        val fromC = OperatorElection().elect(
            "hub-C", lc,
            mapOf("hub-A" to peer(la), "hub-B" to peer(lb)), now,
        )
        assertEquals("hub-B", fromA)
        assertEquals("all hubs converge with no consensus protocol", fromA, fromB)
        assertEquals(fromB, fromC)
    }

    // ---- Hysteresis (flapping protection) --------------------------------

    @Test
    fun aSingleBlipDoesNotSwitchTheOperator() {
        val e = OperatorElection(switchHysteresis = 3)
        // Commit A as operator (A busy, B idle).
        assertEquals("hub-A", e.elect("hub-A", HubLoad(2), mapOf("hub-B" to peer(HubLoad(0))), now))
        // One tick where B is busier -> challenger wins once, but < threshold.
        assertEquals(
            "single blip keeps incumbent",
            "hub-A",
            e.elect("hub-A", HubLoad(0), mapOf("hub-B" to peer(HubLoad(2))), now),
        )
        // Incumbent regains the lead -> challenge collapses.
        assertEquals(
            "hub-A",
            e.elect("hub-A", HubLoad(2), mapOf("hub-B" to peer(HubLoad(0))), now),
        )
    }

    @Test
    fun switchOnlyAfterNConsecutiveChallengerWins() {
        val e = OperatorElection(switchHysteresis = 3)
        assertEquals("hub-A", e.elect("hub-A", HubLoad(5), mapOf("hub-B" to peer(HubLoad(0))), now))
        // B now consistently busier: needs 3 consecutive wins.
        val busyB = mapOf("hub-B" to peer(HubLoad(5)))
        assertEquals("win 1 -> not yet", "hub-A", e.elect("hub-A", HubLoad(0), busyB, now))
        assertEquals("win 2 -> not yet", "hub-A", e.elect("hub-A", HubLoad(0), busyB, now))
        assertEquals("win 3 -> switch", "hub-B", e.elect("hub-A", HubLoad(0), busyB, now))
    }

    @Test
    fun interruptedChallengeStreakResetsAndDoesNotSwitch() {
        val e = OperatorElection(switchHysteresis = 3)
        assertEquals("hub-A", e.elect("hub-A", HubLoad(5), mapOf("hub-B" to peer(HubLoad(0))), now))
        val busyB = mapOf("hub-B" to peer(HubLoad(5)))
        val busyA = mapOf("hub-B" to peer(HubLoad(0)))
        e.elect("hub-A", HubLoad(0), busyB, now) // challenger win 1
        e.elect("hub-A", HubLoad(0), busyB, now) // challenger win 2
        e.elect("hub-A", HubLoad(5), busyA, now) // incumbent wins -> streak reset
        e.elect("hub-A", HubLoad(0), busyB, now) // challenger win 1 again
        assertEquals(
            "streak restarted -> still incumbent after only 2 fresh wins",
            "hub-A",
            e.elect("hub-A", HubLoad(0), busyB, now),
        )
    }

    @Test
    fun aDifferentChallengerEachTickDoesNotAccumulateASwitch() {
        // Two different challengers alternating must NOT add up to a switch:
        // the streak only counts a CONSISTENT challenger.
        val e = OperatorElection(switchHysteresis = 2)
        // A committed (A busiest).
        assertEquals(
            "hub-A",
            e.elect("hub-A", HubLoad(9), mapOf("hub-B" to peer(HubLoad(0)), "hub-C" to peer(HubLoad(0))), now),
        )
        // Tick: B busiest.
        assertEquals(
            "hub-A",
            e.elect("hub-A", HubLoad(0), mapOf("hub-B" to peer(HubLoad(5)), "hub-C" to peer(HubLoad(0))), now),
        )
        // Tick: C busiest (different challenger) -> streak holder changes, count=1.
        assertEquals(
            "alternating challengers do not switch",
            "hub-A",
            e.elect("hub-A", HubLoad(0), mapOf("hub-B" to peer(HubLoad(0)), "hub-C" to peer(HubLoad(5))), now),
        )
    }

    // ---- Age-out ---------------------------------------------------------

    @Test
    fun stalePeerIsExcludedFromCandidates() {
        val e = OperatorElection(staleMs = 15_000L)
        // B would win on load, but its heartbeat is older than the stale window.
        val op = e.elect(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = 1),
            peers = mapOf("hub-B" to peer(HubLoad(queueDepth = 9), ageMs = 20_000L)),
            now = now,
        )
        assertEquals("stale peer aged out -> self is the only candidate", "hub-A", op)
    }

    @Test
    fun freshPeerWithinWindowIsKept() {
        val e = OperatorElection(staleMs = 15_000L)
        val op = e.elect(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = 1),
            peers = mapOf("hub-B" to peer(HubLoad(queueDepth = 9), ageMs = 14_999L)),
            now = now,
        )
        assertEquals("just-inside-window peer counts", "hub-B", op)
    }

    // ---- Clamping (untrusted metrics) ------------------------------------

    @Test
    fun negativeQueueDepthIsClampedToZero() {
        assertEquals(0, OperatorElection.clamp(-50))
    }

    @Test
    fun absurdlyLargeQueueDepthIsClampedToTheCeiling() {
        assertEquals(OperatorElection.MAX_QUEUE_DEPTH, OperatorElection.clamp(Int.MAX_VALUE))
    }

    @Test
    fun clampedPeerCannotForceOperatorStatusWithAnAbsurdValue() {
        val e = OperatorElection()
        // B self-reports Int.MAX_VALUE; A reports a legitimate high value. After
        // clamping B is capped at MAX_QUEUE_DEPTH; A (also above the ceiling)
        // clamps to the same cap, so the tie falls to the smaller id -> A.
        val op = e.elect(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = OperatorElection.MAX_QUEUE_DEPTH),
            peers = mapOf("hub-B" to peer(HubLoad(queueDepth = Int.MAX_VALUE))),
            now = now,
        )
        assertEquals("clamp neutralises the absurd value; tie -> smaller id", "hub-A", op)
    }

    // ---- Single hub (zero-regression case) -------------------------------

    @Test
    fun loneHubElectsItselfAndStays() {
        val e = OperatorElection()
        val op = e.elect("hub-A", HubLoad(queueDepth = 0), emptyMap(), now)
        assertEquals("the only candidate is self", "hub-A", op)
        // Re-running with no peers keeps self; nothing to flap against.
        assertEquals("hub-A", e.elect("hub-A", HubLoad(queueDepth = 7), emptyMap(), now))
    }

    @Test
    fun resetForgetsTheIncumbentSoSelfReclaimsImmediately() {
        // Commit a busy peer B as operator.
        val e = OperatorElection(switchHysteresis = 3)
        assertEquals(
            "hub-B",
            e.elect("hub-A", HubLoad(0), mapOf("hub-B" to peer(HubLoad(5))), now),
        )
        assertEquals("hub-B", e.current())

        // Reconfigure: B is gone. teardown() calls reset() to drop the stale
        // incumbent.
        e.reset()
        assertNull("reset clears the committed operator", e.current())

        // First post-reset election: self is the only/raw winner and must commit
        // IMMEDIATELY, not be treated as a challenger waiting out hysteresis.
        assertEquals(
            "self reclaims at once without dragging the departed operator",
            "hub-A",
            e.elect("hub-A", HubLoad(0), emptyMap(), now),
        )
    }

    @Test
    fun currentReflectsTheCommittedOperatorWithoutRecomputing() {
        val e = OperatorElection()
        assertNull("no election yet -> null", e.current())
        e.elect("hub-A", HubLoad(0), emptyMap(), now)
        assertEquals("hub-A", e.current())
    }

    // ---- Operator-layer 3: dispatch target (advisory least-loaded pick) -----
    //
    // The dispatch hint is the mirror image of the election: the operator points
    // new work at the LEAST loaded live hub. Pure total-order function, so it is
    // deterministic and cross-hub-stable. It only biases claim TIMING — these
    // tests prove the selection, not correctness (the CAS guards that).

    @Test
    fun dispatchTargetIsTheLeastLoadedHub() {
        val target = DispatchTarget.choose(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = 5),
            peers = mapOf(
                "hub-B" to peer(HubLoad(queueDepth = 1)),
                "hub-C" to peer(HubLoad(queueDepth = 3)),
            ),
            now = now,
        )
        assertEquals("idlest hub (B) is the dispatch target", "hub-B", target)
    }

    @Test
    fun dispatchTargetTieIsBrokenByLexicographicallySmallestDeviceId() {
        val target = DispatchTarget.choose(
            selfId = "hub-Z",
            selfLoad = HubLoad(queueDepth = 0),
            peers = mapOf(
                "hub-M" to peer(HubLoad(queueDepth = 0)),
                "hub-A" to peer(HubLoad(queueDepth = 0)),
            ),
            now = now,
        )
        assertEquals("all idle -> smallest id wins", "hub-A", target)
    }

    @Test
    fun dispatchTargetCanBeSelfWhenSelfIsIdlest() {
        val target = DispatchTarget.choose(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = 0),
            peers = mapOf("hub-B" to peer(HubLoad(queueDepth = 4))),
            now = now,
        )
        assertEquals("operator points work at itself when it is idlest", "hub-A", target)
    }

    @Test
    fun dispatchTargetIsNullWithNoLivePeers() {
        // A lone hub needs no hint: it is trivially its own target and claims now.
        assertNull(DispatchTarget.choose("hub-A", HubLoad(0), emptyMap(), now))
        // A peer older than the stale window is aged out -> no live peer -> null.
        assertNull(
            DispatchTarget.choose(
                selfId = "hub-A",
                selfLoad = HubLoad(0),
                peers = mapOf("hub-B" to peer(HubLoad(0), ageMs = 20_000L)),
                now = now,
                staleMs = 15_000L,
            ),
        )
    }

    @Test
    fun dispatchTargetAllHubsAgreeFromTheSamePicture() {
        // Same global picture from each hub's vantage -> identical target (the
        // split-brain-free property, mirrored for the hint).
        val la = HubLoad(queueDepth = 4)
        val lb = HubLoad(queueDepth = 1)
        val lc = HubLoad(queueDepth = 1)
        val fromA = DispatchTarget.choose("hub-A", la, mapOf("hub-B" to peer(lb), "hub-C" to peer(lc)), now)
        val fromB = DispatchTarget.choose("hub-B", lb, mapOf("hub-A" to peer(la), "hub-C" to peer(lc)), now)
        val fromC = DispatchTarget.choose("hub-C", lc, mapOf("hub-A" to peer(la), "hub-B" to peer(lb)), now)
        assertEquals("idlest, tie to smallest id", "hub-B", fromA)
        assertEquals(fromA, fromB)
        assertEquals(fromB, fromC)
    }

    @Test
    fun dispatchTargetClampsUntrustedQueueDepth() {
        // A peer self-reporting a negative depth cannot appear "more idle" than 0.
        val target = DispatchTarget.choose(
            selfId = "hub-A",
            selfLoad = HubLoad(queueDepth = 0),
            peers = mapOf("hub-B" to peer(HubLoad(queueDepth = -100))),
            now = now,
        )
        // B clamps to 0, tying with self (also 0) -> smaller id "hub-A" wins;
        // the spoofed negative did NOT let B steal the target slot for free.
        assertEquals("hub-A", target)
    }
}
