package ai.liquidway.lfmsmoke.ai

import ai.liquidway.lfmsmoke.data.Message

/**
 * The layer-4 abstraction boundary for on-device AI summarisation.
 *
 * Why an interface: the production path runs a heavyweight LFM through the LEAP
 * SDK (slow, non-deterministic, emulator-hostile). The deterministic JVM tests
 * need a synchronous, predictable stand-in. Hiding the engine behind this
 * single suspend method lets [ai.liquidway.lfmsmoke.net.MeshController] stay
 * engine-agnostic and lets the tests inject a fake without touching LEAP.
 *
 * Contract:
 *  - [summarize] takes the recent chat window (oldest-first) and returns a
 *    single human-readable summary string.
 *  - Implementations may be expensive; the caller is responsible for running
 *    this off the chat relay path (see MeshController's summary dispatcher).
 *  - An empty input must still return a non-blank, sensible string (so the UI
 *    never shows an empty AI bubble).
 */
interface SummarizationEngine {

    /**
     * Produces a situational summary of [messages].
     *
     * @param messages recent chat messages, oldest-first, already capped by the
     *   caller. Never null; may be empty.
     * @return a non-blank summary line.
     */
    suspend fun summarize(messages: List<Message>): String

    /**
     * Releases any heavyweight resources (e.g. the loaded LFM) so memory is
     * returned while idle. Idempotent; the next [summarize] must transparently
     * re-acquire what it needs. Default no-op: lightweight test fakes hold
     * nothing to free, so they need not override this.
     */
    suspend fun release() {}

    companion object {
        /**
         * Upper bound on how many recent messages are fed to the model. Kept in
         * the same order of magnitude as the layer-3 backfill cap so a summary
         * reflects the catch-up window a freshly-joined device would have.
         */
        const val MAX_SUMMARY_MESSAGES = 50

        /**
         * Builds the prompt fed to the model from a chat window. Pure and
         * deterministic so it can be unit-tested independently of any engine.
         */
        fun buildPrompt(messages: List<Message>): String {
            if (messages.isEmpty()) {
                return "No chat messages have been exchanged yet. " +
                    "Reply with a single short sentence stating there is nothing to summarise."
            }
            val transcript = messages.joinToString("\n") { m ->
                "${m.senderName}: ${m.body}"
            }
            return buildString {
                append("You are an assistant on an offline mesh chat used during ")
                append("a disaster or connectivity outage. Summarise the situation ")
                append("from the conversation below in 2-4 concise sentences. ")
                append("Focus on needs, locations, people, and decisions. ")
                append("Do not invent facts not present in the messages.\n\n")
                append("Conversation:\n")
                append(transcript)
                append("\n\nSummary:")
            }
        }
    }
}
