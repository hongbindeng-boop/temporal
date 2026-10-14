package prospecting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Activity implementations.
 * runFlowAgent uses the real Google ADK agent via FlowAgentRunner.
 * All other methods are stubs for DB/notification calls.
 */
public class ProspectingActivitiesImpl implements ProspectingActivities {

    // Shared runner — stateless, safe to reuse across activity executions.
    // In Gong prod: InstructionAgentService is a Spring singleton injected here.
    private final FlowAgentRunner agentRunner = new FlowAgentRunner();

    // ponytail: POC-only in-memory history. In prod, load from Postgres inside runFlowAgent
    // (design doc §8) so Temporal replay never re-runs DB reads. Ceiling: lost on worker restart.
    private final ConcurrentHashMap<String, List<Map<String, Object>>> historyByWorkflow = new ConcurrentHashMap<>();

    @Override
    public void persistSession(String workflowId, String goal) {
        System.out.printf("[DB] INSERT prospecting_session workflowId=%s goal='%s'%n", workflowId, goal);
        historyByWorkflow.put(workflowId, new ArrayList<>());
    }

    @Override
    public FlowAgentResult runFlowAgent(AgentContext context) {
        System.out.printf("[ADK] reasoning cycle — lastEvent=%s step=%d touches=%d%n",
                context.lastEvent, context.stepIndex, context.touchCount);
        try {
            // Load prior reasoning steps so the agent can build on its own decisions.
            // In prod: SELECT * FROM prospecting_reasoning_step WHERE workflow_id = ? ORDER BY step_index
            List<Map<String, Object>> history = historyByWorkflow.getOrDefault(context.workflowId, List.of());
            return agentRunner.run(context, history);
        } catch (Exception e) {
            System.err.printf("[ADK] agent failed: %s%n", e.getMessage());
            return new FlowAgentResult(Outcome.ERROR, null, null, null, e.getMessage());
        }
    }

    @Override
    public void persistReasoningStep(String workflowId, int stepIndex, FlowAgentResult result) {
        System.out.printf("[DB] INSERT prospecting_reasoning_step workflowId=%s step=%d outcome=%s action=%s%n",
                workflowId, stepIndex, result.outcome(), result.action());
        // Accumulate so subsequent runFlowAgent calls see prior steps in DECISION_LOG.
        historyByWorkflow.computeIfAbsent(workflowId, k -> new ArrayList<>())
                .add(Map.of(
                        "step",      stepIndex,
                        "action",    result.action()    != null ? result.action()    : "",
                        "outcome",   result.outcome().name(),
                        "reasoning", result.reasoning() != null ? result.reasoning() : ""
                ));
    }

    @Override
    public void executeAction(String workflowId, String action, String detail) {
        // In Gong prod: route to EmailService, DialerService, etc.
        System.out.printf("[ACTION] workflowId=%s action=%s detail='%s'%n", workflowId, action,
                detail != null ? detail.replace('\n', ' ') : "");
    }

    @Override
    public void updateSessionStatus(String workflowId, String status) {
        System.out.printf("[DB] UPDATE prospecting_session status=%s workflowId=%s%n", status, workflowId);
    }

    @Override
    public void notifyRepTimeout(String workflowId) {
        System.out.printf("[NOTIFY] Rep: manual step timed out workflowId=%s%n", workflowId);
    }

    @Override
    public void notifyRepAgentFailed(String workflowId, String message) {
        System.out.printf("[NOTIFY] Rep: agent failed workflowId=%s error=%s%n", workflowId, message);
    }
}
