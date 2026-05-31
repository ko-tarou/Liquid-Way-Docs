package ai.liquidway.lfmsmoke.net

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * Unit proof of the Stage-1 bridge forwarding policy ([BridgePolicy]).
 *
 * Pure logic, no transport: this PR adds the loop-prevention decision but does
 * NOT wire it into any relay path (the star topology is unchanged). These tests
 * are therefore the only thing that exercises it until PR#3 calls it from the
 * relay. They assert the two guards — the seen-set loop break and the [MAX_HOP]
 * ceiling — plus bounded-set eviction and thread-safety.
 */
class BridgePolicyTest {

    @Test
    fun firstSightingOfAnIdReturnsHopPlusOne() {
        val policy = BridgePolicy()
        assertEquals(1, policy.bridgeHopFor("m1", 0))
    }

    @Test
    fun secondSightingOfSameIdIsRefused() {
        val policy = BridgePolicy()
        assertEquals(1, policy.bridgeHopFor("m1", 0)) // first forward
        assertNull("repeat id must not re-forward (loop break)", policy.bridgeHopFor("m1", 0))
    }

    @Test
    fun hopAtOrAboveMaxIsRefusedAndBoundaryIsExact() {
        val policy = BridgePolicy()
        // hop = 1 is still under the ceiling -> forwarded as hop 2.
        assertEquals(2, policy.bridgeHopFor("under", 1))
        // hop = MAX_HOP (2) is the ceiling -> refused.
        assertNull("hop == MAX_HOP is the ceiling", policy.bridgeHopFor("at-max", BridgePolicy.MAX_HOP))
        // hop above the ceiling is likewise refused.
        assertNull("hop > MAX_HOP is refused", policy.bridgeHopFor("over-max", 3))
    }

    @Test
    fun ceilingGuardTakesPrecedenceWithoutRecordingTheId() {
        val policy = BridgePolicy()
        // An over-ceiling id is refused...
        assertNull(policy.bridgeHopFor("c1", BridgePolicy.MAX_HOP))
        // ...and was NOT recorded, so a later in-range sighting still forwards.
        assertEquals(1, policy.bridgeHopFor("c1", 0))
    }

    @Test
    fun distinctIdsDoNotInterfereWithEachOther() {
        val policy = BridgePolicy()
        assertEquals(1, policy.bridgeHopFor("a", 0))
        assertEquals(1, policy.bridgeHopFor("b", 0))
        assertEquals(2, policy.bridgeHopFor("c", 1))
        // Re-asking each refuses independently.
        assertNull(policy.bridgeHopFor("a", 0))
        assertNull(policy.bridgeHopFor("b", 0))
        assertNull(policy.bridgeHopFor("c", 1))
    }

    @Test
    fun boundedSetEvictsOldestOnceCapacityIsExceeded() {
        // Tiny capacity so eviction is easy to drive.
        val policy = BridgePolicy(capacity = 2)
        assertEquals(1, policy.bridgeHopFor("old", 0))   // seen = {old}
        assertEquals(1, policy.bridgeHopFor("mid", 0))   // seen = {old, mid}
        assertEquals(1, policy.bridgeHopFor("new", 0))   // overflow -> evicts "old"
        // "old" fell out of the set, so it is treated as never-seen again.
        assertEquals("evicted id is forwardable again", 1, policy.bridgeHopFor("old", 0))
        // "new" is still resident, so a repeat is still refused.
        assertNull("resident id stays refused", policy.bridgeHopFor("new", 0))
    }

    @Test
    fun evictionIsInsertionOrderedAndRefusedLookupDoesNotReorder() {
        // The lookup uses containsKey (not get), which does NOT count as an
        // access, so a refused repeat never refreshes recency: eviction follows
        // pure insertion order of *forwarded* ids (oldest forwarded falls out).
        val policy = BridgePolicy(capacity = 2)
        policy.bridgeHopFor("a", 0)             // forwarded -> [a]
        policy.bridgeHopFor("b", 0)             // forwarded -> [a, b]
        assertNull(policy.bridgeHopFor("a", 0)) // refused; does NOT touch "a"
        policy.bridgeHopFor("c", 0)             // overflow evicts oldest "a" -> [b, c]
        assertEquals("'a' was evicted, forwardable again", 1, policy.bridgeHopFor("a", 0))
        // "c" was never evicted, so it stays refused.
        assertNull("'c' still resident", policy.bridgeHopFor("c", 0))
    }

    @Test
    fun concurrentForwardsOfTheSameIdYieldExactlyOneNonNull() {
        // Thread-safety: N threads race to forward the same id; the seen-set
        // must let exactly one through and refuse the rest (no double-forward).
        val policy = BridgePolicy()
        val threads = 16
        val pool = Executors.newFixedThreadPool(threads)
        val start = CountDownLatch(1)
        val nonNull = AtomicInteger(0)
        repeat(threads) {
            pool.submit {
                start.await()
                if (policy.bridgeHopFor("hot", 0) != null) nonNull.incrementAndGet()
            }
        }
        start.countDown()
        pool.shutdown()
        pool.awaitTermination(5, TimeUnit.SECONDS)
        assertEquals("exactly one thread forwards the id", 1, nonNull.get())
    }
}
