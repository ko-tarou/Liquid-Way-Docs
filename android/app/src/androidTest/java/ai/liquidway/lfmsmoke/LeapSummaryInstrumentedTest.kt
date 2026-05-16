package ai.liquidway.lfmsmoke

import ai.liquidway.lfmsmoke.ai.LeapSummarizationEngine
import ai.liquidway.lfmsmoke.data.Message
import ai.liquidway.lfmsmoke.data.MessageStatus
import android.util.Log
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Exercises the REAL LEAP path end-to-end through [LeapSummarizationEngine]
 * (which composes the proven [LfmEngine]). This downloads LFM2-350M on first
 * run and performs on-device inference, so it is slow and heavy — it is the
 * production validation, kept separate from the deterministic JVM tests.
 *
 * The emulator may not complete real-model generation in a reasonable time;
 * the honest validation path is a physical arm64-v8a device. The deterministic
 * proof of the layer-4 protocol/relay/priority lives in MeshRelayJvmTest.
 */
@RunWith(AndroidJUnit4::class)
class LeapSummaryInstrumentedTest {

    @Test
    fun realLeapEngineProducesANonBlankSummaryOnce() = runBlocking {
        val ctx = InstrumentationRegistry.getInstrumentation().targetContext
        val engine = LeapSummarizationEngine(ctx) { p ->
            Log.i("LiqMesh/AI", "model download ${(p * 100).toInt()}%")
        }
        val window = listOf(
            Message("m1", "alice", "Alice", "We need water at the north shelter", 1_000, MessageStatus.SENT),
            Message("m2", "bob", "Bob", "Two people are injured near the bridge", 2_000, MessageStatus.SENT),
            Message("m3", "carol", "Carol", "I can bring a first-aid kit in 20 min", 3_000, MessageStatus.SENT),
        )
        val summary = engine.summarize(window)
        Log.i("LiqMesh/AI", "REAL LEAP SUMMARY = $summary")
        assertTrue("summary must be non-blank", summary.isNotBlank())
    }
}
