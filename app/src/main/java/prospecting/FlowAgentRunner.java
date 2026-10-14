package prospecting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.adk.agents.LlmAgent;
import com.google.adk.runner.InMemoryRunner;
import com.google.adk.sessions.Session;
import com.google.genai.types.Content;
import com.google.genai.types.Part;
import com.google.genai.types.Schema;
import com.google.genai.types.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mimics Gong's InstructionAgentService — stripped to POC scope.
 *
 * Gong's full stack:
 *   LlmAgentBuilderHelper.createAgentBuilder()   → builds LlmAgent from prompt YAML + tools
 *   GongRunnerProvider.createPersistent()         → builds Runner with DB session + plugins
 *   BulkRequestFlow.runUserMessage()              → streams events, extracts state variables
 *
 * Here: InMemoryRunner (no DB), no plugins, no tools — pure reasoning.
 * In prod swap InMemoryRunner → Runner with DbBasedSessionService so the session
 * (including DECISION_LOG state) survives worker restarts (design doc §8).
 */
public class FlowAgentRunner {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Enforces clean JSON output at the API level — no markdown fences possible.
    // Gong prod equivalent: LlmAgentBuilderHelper passes responseSchema via GenerationConfig.
    private static final Schema OUTPUT_SCHEMA = Schema.builder()
            .type(new Type(Type.Known.OBJECT))
            .properties(Map.of(
                    "reasoning", Schema.builder().type(new Type(Type.Known.STRING)).build(),
                    "action",    Schema.builder().type(new Type(Type.Known.STRING))
                                     .enum_(List.of("SEND_EMAIL", "MAKE_CALL", "WAIT", "DONE")).build(),
                    "detail",    Schema.builder().type(new Type(Type.Known.STRING)).build(),
                    "outcome",   Schema.builder().type(new Type(Type.Known.STRING))
                                     .enum_(List.of("WAIT", "MANUAL_STEP_PENDING", "DONE", "ERROR")).build()
            ))
            .required(List.of("reasoning", "action", "detail", "outcome"))
            .build();

    // In Gong prod this is loaded from prospecting-orchestrator.yaml via LlmAgentBuilderHelper.
    private static final String SYSTEM_PROMPT = """
            You are a prospecting flow agent. Your job is to determine the single best next step
            that moves the rep closer to achieving their GOAL.

            Think goal-first on every cycle — in this order:

              1. DECOMPOSE THE GOAL
                 What does successfully completing GOAL look like?
                 What is the typical sequence of steps required? (e.g., for "book a demo":
                   intro email → follow-up if no reply → positive reply → send calendar link → meeting booked)

              2. ASSESS PROGRESS
                 Look at DECISION_LOG. How many touches have been made? What was the last action?
                 Are we making forward progress toward the goal, stuck, or losing the prospect?

              3. PICK THE BEST NEXT STEP
                 Given where we are in the sequence and LAST_EVENT, what single step has the highest
                 probability of advancing toward the goal right now?
                 - If no response after an email → is a follow-up or a different channel more likely to work?
                 - If a positive reply → move to the conversion step (send meeting link, not another intro)
                 - If OOO → compute when they're back, WAIT until then (don't waste a touch)
                 - If meeting booked → DONE, goal achieved

              4. CHECK GUARDRAILS
                 Does this step violate GUARDRAILS (max touches, allowed channels)?
                 If guardrail is hit → DONE (not ERROR).

            Session state you have access to:
              GOAL         — what the rep wants to achieve
              GUARDRAILS   — constraints (channels allowed, max touches, cadence rules)
              DECISION_LOG — JSON array of all prior reasoning cycles (action, reasoning, outcome per step)
              LAST_EVENT   — what just triggered this cycle (START | EMAIL_REPLY:... | MEETING_BOOKED | TIMEOUT)

            Available actions:
              SEND_EMAIL  — send a personalised email immediately
              MAKE_CALL   — schedule or make a call to the prospect
              WAIT        — no action now; sleep until the next event or timeout
              DONE        — goal achieved (or guardrail hit), stop the workflow

            Outcome field maps action to workflow routing:
              SEND_EMAIL with agent auto-sending → outcome: WAIT (wait for reply)
              SEND_EMAIL as draft for rep to review → outcome: MANUAL_STEP_PENDING
              MAKE_CALL scheduled → outcome: WAIT
              WAIT → outcome: WAIT
              DONE → outcome: DONE

            Your response fields:
              reasoning — explain: what does the goal require, what has been done, why this step is best
              action    — one of: SEND_EMAIL, MAKE_CALL, WAIT, DONE
              detail    — email subject + body, or call plan; empty string for WAIT/DONE
              outcome   — one of: WAIT, MANUAL_STEP_PENDING, DONE, ERROR
            """;

    /**
     * One reasoning cycle — called inside runFlowAgentActivity.
     *
     * @param context  AgentContext (workflowId, goal, guardrails, lastEvent)
     * @param history  last N ProspectingReasoningStep rows from Postgres (design doc §8).
     *                 Loaded here inside the activity so Temporal replay never re-runs the DB read.
     * @return         FlowAgentResult with outcome for the Temporal workflow switch statement
     */
    public FlowAgentResult run(AgentContext context, List<Map<String, Object>> history) throws Exception {

        // ── 1. Build the agent ──────────────────────────────────────────────
        // Gong: LlmAgentBuilderHelper.createAgentBuilder(name, promptYaml, config, tools, toolConfig)
        // For POC: no tools. Post-POC add email-writing skill, callRuleset tool.
        LlmAgent agent = LlmAgent.builder()
                .name("prospecting-flow-agent")
                .model("gemini-2.5-flash")
                .instruction(SYSTEM_PROMPT)
                .outputSchema(OUTPUT_SCHEMA)   // forces JSON at API level, no markdown fences
                .build();

        // ── 2. Build the runner ─────────────────────────────────────────────
        // Gong: GongRunnerProvider.createPersistent(agent, aiConfig) + plugins
        // InMemoryRunner = ephemeral session, no DB. Fine for POC.
        // Signature: InMemoryRunner(BaseAgent agent, String appName)
        InMemoryRunner runner = new InMemoryRunner(agent, "prospecting");

        // ── 3. Create session with context ──────────────────────────────────
        // Gong: AgentExecutor.createStateDeltaForInvocation() populates session state.
        // createSession(appName, userId, initialState, sessionId)
        Map<String, Object> initialState = new HashMap<>();
        initialState.put("GOAL",         context.goal);
        initialState.put("GUARDRAILS",   context.guardrails);
        initialState.put("DECISION_LOG", MAPPER.writeValueAsString(history));
        initialState.put("LAST_EVENT",   context.lastEvent);

        Session session = runner.sessionService()
                .createSession("prospecting", context.workflowId, initialState, context.workflowId)
                .blockingGet();

        // ── 4. Run and stream events ────────────────────────────────────────
        // Gong: BulkRequestFlow.runUserMessage() → Flowable<Event> → iterate and collect text
        // runAsync(userId, sessionId, userMessage) — simplest overload
        String userPrompt = "Goal: " + context.goal
                + "\nGuardrails: " + context.guardrails
                + "\nCurrent step: " + context.stepIndex
                + " | Outreach touches so far: " + context.touchCount
                + "\nLast event: " + context.lastEvent
                + "\nDecision log (" + history.size() + " prior steps): "
                + MAPPER.writeValueAsString(history)
                + "\n\nDecompose the goal, assess progress from the decision log, then decide the best next step.";

        // Content.fromParts(Part.fromText(...)) — static factory methods (real API)
        Content userMessage = Content.fromParts(Part.fromText(userPrompt));

        StringBuilder responseText = new StringBuilder();

        runner.runAsync(context.workflowId, session.id(), userMessage)
                .blockingForEach(event -> {
                    // Collect text from all events — ADK streams content in intermediate
                    // events; the turnComplete=true event is just a termination signal
                    // with no content. Gong prod reads FLOW_OUTCOME from session state
                    // after the stream ends instead.
                    if (event.content().isPresent()) {
                        event.content().get().parts().ifPresent(parts ->
                                parts.forEach(p -> p.text().ifPresent(responseText::append)));
                    }
                });

        // ── 5. Parse structured output ──────────────────────────────────────
        // Gong: InstructionAgentService extracts FLOW_OUTCOME from session state.
        // Here we parse the JSON the LLM returned directly.
        Map<String, String> parsed = MAPPER.readValue(responseText.toString(), Map.class);

        System.out.println("[ADK] reasoning: " + parsed.get("reasoning"));
        System.out.println("[ADK] action:    " + parsed.get("action"));
        System.out.println("[ADK] outcome:   " + parsed.get("outcome"));

        Outcome outcome = Outcome.valueOf(parsed.getOrDefault("outcome", "WAIT"));
        return new FlowAgentResult(outcome, parsed.get("action"), parsed.get("detail"), parsed.get("reasoning"), null);
    }
}
