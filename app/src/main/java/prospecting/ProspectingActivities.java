package prospecting;

import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;

/**
 * Every side effect lives here — DB writes, LLM calls, notifications.
 * The workflow calls these; Temporal retries them automatically on failure.
 */
@ActivityInterface
public interface ProspectingActivities {

    @ActivityMethod
    void persistSession(String workflowId, String goal);

    /** Runs the LLM reasoning cycle. Returns WAIT | DONE | MANUAL_STEP_PENDING | ERROR. */
    @ActivityMethod
    FlowAgentResult runFlowAgent(AgentContext context);

    /** Persists one reasoning step to Postgres — separate from runFlowAgent so a DB
     *  failure retries only this cheap write, not the expensive LLM call. */
    @ActivityMethod
    void persistReasoningStep(String workflowId, int stepIndex, FlowAgentResult result);

    @ActivityMethod
    void updateSessionStatus(String workflowId, String status);

    @ActivityMethod
    void notifyRepTimeout(String workflowId);

    @ActivityMethod
    void notifyRepAgentFailed(String workflowId, String message);

    /**
     * Executes the action the agent chose — routes to email/dialer/etc.
     * Separated from runFlowAgent so an execution failure never retries the LLM call.
     */
    @ActivityMethod
    void executeAction(String workflowId, String action, String detail);
}
