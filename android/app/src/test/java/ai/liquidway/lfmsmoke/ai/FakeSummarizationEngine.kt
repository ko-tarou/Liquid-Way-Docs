package ai.liquidway.lfmsmoke.ai

import ai.liquidway.lfmsmoke.data.Message
import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.atomic.AtomicInteger

/**
 * Deterministic [SummarizationEngine] for JVM tests.
 *
 * It never touches LEAP, so the layer-4 protocol/relay/dedup/priority logic can
 * be proven with zero non-determinism. The produced text is a fixed-format
 * digest of the input so assertions are exact.
 *
 * [gate] optionally blocks generation until released, letting a test interleave
 * a slow summary with fast plain-chat traffic to prove "chat first": plain
 * messages must keep relaying while [summarize] is parked here.
 */
class FakeSummarizationEngine(
    private val gate: CompletableDeferred<Unit>? = null,
) : SummarizationEngine {

    val callCount = AtomicInteger(0)

    @Volatile
    var lastWindowSize: Int = -1
        private set

    override suspend fun summarize(messages: List<Message>): String {
        callCount.incrementAndGet()
        lastWindowSize = messages.size
        gate?.await()
        if (messages.isEmpty()) return "SUMMARY: (no messages)"
        val joined = messages.joinToString("; ") { "${it.senderName}:${it.body}" }
        return "SUMMARY[${messages.size}] $joined"
    }
}
