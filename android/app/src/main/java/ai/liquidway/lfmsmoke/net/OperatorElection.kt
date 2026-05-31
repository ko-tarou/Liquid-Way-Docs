package ai.liquidway.lfmsmoke.net

/**
 * Operator-layer 2: **deterministic, vote-free operator election** with
 * flapping (hysteresis) protection.
 *
 * THE DESIGN IN ONE LINE: nobody votes. Every hub looks at the *same* load
 * picture (each hub's load is gossiped for free on the bridge heartbeat, see
 * [HubLoad] / `onPeerLoad`) and runs the *same* deterministic rule, so all
 * hubs independently compute the *same* operator — no consensus protocol, no
 * leader-election round-trips, and therefore no split-brain by construction.
 * The only way two hubs disagree is if they see different inputs (a heartbeat
 * still in flight), and that resolves itself on the next gossip tick.
 *
 * WHAT THIS PR DOES (and only this): it *computes* who the operator is and
 * exposes it. It does NOT change routing, summary ownership, or anything a hub
 * actually does. Acting on the operator (dispatch hints) is the next PR. So
 * wiring this in is behaviour-neutral.
 *
 * ── The election rule (deterministic) ────────────────────────────────────
 *  - Candidate set = self + every peer last heard within [staleMs] (a peer
 *    whose heartbeat went quiet is *aged out*, so a dead hub cannot stay
 *    operator forever).
 *  - Winner = the candidate with the **highest [HubLoad.queueDepth]**; ties
 *    broken by the **lexicographically smallest deviceId** (total order ⇒
 *    deterministic).
 *
 *    Why "busiest hub wins"? This follows the product design: the hub that is
 *    most loaded becomes the *operator* (the dispatcher) so it can shed / route
 *    its own backlog to idler peers, rather than the idlest hub having to pull
 *    work it cannot see. The rule itself is arbitrary from a correctness
 *    standpoint — ANY total-order rule gives split-brain-free agreement — but
 *    this one matches the intended future dispatch semantics.
 *
 * ── Hysteresis (flapping protection) ─────────────────────────────────────
 *  Re-running the rule on every heartbeat would let the operator role
 *  oscillate as queueDepth jitters between 0 and 1. So a *committed* operator
 *  is only replaced once a DIFFERENT challenger has won the raw election
 *  [switchHysteresis] times **in a row**. A single blip (challenger wins once,
 *  then the incumbent wins again) resets the streak and changes nothing.
 *
 *  This also encodes the product's "step aside after it's been the dispatcher
 *  long enough" intuition as a simple consecutive-count threshold rather than a
 *  dispatchCount cooldown: a count threshold is self-evidently correct, needs
 *  no clock, and is trivially unit-testable, whereas a dispatchCount cooldown
 *  would couple election to an unbounded monotonic counter and a tuning value
 *  with no obvious right answer. Consecutive-count is the simpler primitive and
 *  is enough to kill flapping.
 *
 * ── Untrusted metrics ────────────────────────────────────────────────────
 *  queueDepth/dispatchCount are a peer's *self-report* over the LAN and feed
 *  the election, so they are clamped to a sane range ([clamp]) before use: a
 *  negative or absurdly large queueDepth cannot let a peer force (or dodge)
 *  operator status with an out-of-band value. deviceId spoofing is still
 *  possible and is accepted under the Stage-1 same-LAN trust model; authenticated
 *  identity is future work (Task #21).
 *
 * Pure logic, no Room / Context / transport dependency — exactly like
 * [BridgePolicy] / [QuestionOwnership] — so determinism, agreement, hysteresis,
 * age-out and clamping are all unit-tested directly. Thread-safety: [elect] is
 * the only mutating entry point and is [Synchronized]; the controller may call
 * it from concurrent reader coroutines (one per heartbeat).
 */
class OperatorElection(
    private val staleMs: Long = DEFAULT_PEER_STALE_MS,
    private val switchHysteresis: Int = DEFAULT_SWITCH_HYSTERESIS,
) {
    /** A single hub in the candidate set: who it is and its (clamped) load. */
    data class Candidate(val deviceId: String, val queueDepth: Int)

    // The currently COMMITTED operator (what the world observes). Null until the
    // first election. Only [elect] mutates it, under the lock.
    private var committed: String? = null

    // How many consecutive elections a NON-incumbent challenger has won. Reset
    // to 0 whenever the incumbent wins (or we have no incumbent yet / commit).
    private var challengerStreak: Int = 0

    // The challenger that has been winning consecutively, tracked so a *different*
    // challenger each tick does not accumulate a streak toward a switch.
    private var streakHolder: String? = null

    /**
     * Recompute the operator from the current load picture and return the
     * COMMITTED operator id (after hysteresis), or null if there are no live
     * candidates at all (cannot happen when self is always included).
     *
     * @param selfId   this hub's deviceId (always a candidate).
     * @param selfLoad this hub's current load.
     * @param peers    every peer's last-reported load + when it was seen.
     * @param now      current time (epoch ms); a peer older than [staleMs] is
     *                 excluded (age-out).
     */
    @Synchronized
    fun elect(
        selfId: String,
        selfLoad: HubLoad,
        peers: Map<String, MeshController.PeerLoad>,
        now: Long,
    ): String? {
        val candidates = buildCandidates(selfId, selfLoad, peers, now)
        val rawWinner = pickWinner(candidates) ?: return committed

        val current = committed
        if (current == null) {
            // First election: commit immediately, nothing to flap against.
            committed = rawWinner
            resetStreak()
            return committed
        }
        if (rawWinner == current) {
            // Incumbent still wins -> any in-progress challenge collapses.
            resetStreak()
            return committed
        }
        // A challenger won this tick. Only honour a *consistent* challenger:
        // a different challenger each tick must not accumulate a switch.
        if (rawWinner == streakHolder) {
            challengerStreak++
        } else {
            streakHolder = rawWinner
            challengerStreak = 1
        }
        if (challengerStreak >= switchHysteresis) {
            committed = rawWinner
            resetStreak()
        }
        return committed
    }

    /** The committed operator without recomputing (observable snapshot). */
    @Synchronized
    fun current(): String? = committed

    /**
     * Forget all committed/streak state so the *next* [elect] starts from a
     * clean slate. Call this on teardown/reconfigure: otherwise a stale
     * incumbent (a peer that no longer exists after the reconfigure) stays in
     * [committed], and the first post-reconfigure [elect] would treat self as a
     * mere challenger — re-publishing the departed operator id and taking
     * [switchHysteresis] ticks before self can reclaim the role. Resetting keeps
     * the published operator in step with the live candidate set.
     */
    @Synchronized
    fun reset() {
        committed = null
        resetStreak()
    }

    private fun resetStreak() {
        challengerStreak = 0
        streakHolder = null
    }

    /**
     * Build the live candidate set: self (always) + peers seen within [staleMs].
     * queueDepth is clamped here so every downstream comparison is over sane
     * values regardless of what a peer self-reported.
     */
    private fun buildCandidates(
        selfId: String,
        selfLoad: HubLoad,
        peers: Map<String, MeshController.PeerLoad>,
        now: Long,
    ): List<Candidate> {
        val out = ArrayList<Candidate>(peers.size + 1)
        out += Candidate(selfId, clamp(selfLoad.queueDepth))
        for ((id, pl) in peers) {
            if (id == selfId) continue // never double-count self
            if (now - pl.seenAt > staleMs) continue // age-out a silent peer
            out += Candidate(id, clamp(pl.load.queueDepth))
        }
        return out
    }

    /**
     * The deterministic rule: highest clamped queueDepth wins; ties broken by
     * the lexicographically smallest deviceId. Given the same candidate set on
     * any hub, this returns the same id — the agreement property.
     */
    private fun pickWinner(candidates: List<Candidate>): String? =
        candidates.maxWithOrNull(
            compareBy<Candidate> { it.queueDepth }.thenByDescending { it.deviceId },
        )?.deviceId

    companion object {
        /**
         * A peer not heard from within this window is aged out of the candidate
         * set. Comfortably longer than the bridge heartbeat so a single missed
         * beat does not drop a live peer, short enough that a truly dead hub
         * stops being eligible quickly.
         */
        const val DEFAULT_PEER_STALE_MS = 15_000L

        /**
         * Consecutive raw-election wins a challenger needs before it unseats the
         * committed operator. 3 absorbs short load blips while still switching
         * promptly on a sustained shift.
         */
        const val DEFAULT_SWITCH_HYSTERESIS = 3

        /**
         * Sane upper bound for an untrusted self-reported queueDepth. With
         * single-flight summarisation the honest value is 0 or 1; this ceiling
         * still admits a more elaborate future queue while refusing an absurd
         * value a malicious/buggy peer might send to force operator status.
         */
        const val MAX_QUEUE_DEPTH = 1_000

        /** Clamp an untrusted queueDepth into [0, [MAX_QUEUE_DEPTH]]. */
        fun clamp(queueDepth: Int): Int = queueDepth.coerceIn(0, MAX_QUEUE_DEPTH)
    }
}

/**
 * Operator-layer 3: **the advisory dispatch target** — pure, vote-free selection
 * of the hub the operator wants to preferentially claim a new summary.
 *
 * THE RULE (deterministic, mirror image of the election): the **least loaded**
 * live candidate — lowest clamped [HubLoad.queueDepth], ties broken by the
 * **lexicographically smallest deviceId** — so the busiest hub (the operator)
 * sheds new work to the idlest peer. Like [OperatorElection] this is a pure
 * total-order function over the same gossiped picture, so it is deterministic
 * and unit-testable in isolation.
 *
 * It is ONLY consumed as a hint: the chosen target gets a zero-delay claim while
 * other hubs wait a short fallback window before claiming. The claim CAS
 * ([QuestionOwnership]) remains the correctness floor — the hint can only change
 * *who claims first*, never whether a question ends up single-owned, and if the
 * target never claims another hub still claims after the fallback (liveness).
 *
 * Untrusted: queueDepth/deviceId are peer self-reports; clamping bounds the load
 * but a spoofed id could mis-steer the hint. Accepted under the Stage-1 same-LAN
 * trust model (Task #21) — a bad hint at worst lowers efficiency, never breaks
 * single ownership or liveness.
 */
object DispatchTarget {

    /**
     * Pick the least-loaded live hub from [selfId]+[peers] (peers older than
     * [staleMs] are aged out, mirroring the election). Returns the chosen
     * deviceId, or null when there is no peer at all (a lone hub needs no hint —
     * it is trivially its own target and claims immediately anyway).
     */
    fun choose(
        selfId: String,
        selfLoad: HubLoad,
        peers: Map<String, MeshController.PeerLoad>,
        now: Long,
        staleMs: Long = OperatorElection.DEFAULT_PEER_STALE_MS,
    ): String? {
        val live = peers.entries.filter { (id, pl) ->
            id != selfId && now - pl.seenAt <= staleMs
        }
        // No live peer -> no meaningful hint (self is the only candidate).
        if (live.isEmpty()) return null
        var bestId = selfId
        var bestDepth = OperatorElection.clamp(selfLoad.queueDepth)
        for ((id, pl) in live) {
            val depth = OperatorElection.clamp(pl.load.queueDepth)
            // Strictly-less wins; on a tie keep the lexicographically smaller id
            // for a deterministic, cross-hub-stable choice.
            if (depth < bestDepth || (depth == bestDepth && id < bestId)) {
                bestId = id
                bestDepth = depth
            }
        }
        return bestId
    }
}
