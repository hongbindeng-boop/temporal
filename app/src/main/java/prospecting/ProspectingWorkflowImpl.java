package prospecting;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.Workflow;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * LEARNING FILE — maps directly to section 7 of the design doc.
 *
 * Rules:
 *   - Zero DB calls, zero HTTP calls, zero parsing in this class.
 *   - Every side effect is an activity call (retried by Temporal automatically).
 *   - Signal handlers only flip boolean flags or set a field — never call activities.
 *   - Workflow.await() is how we sleep: the worker thread is freed, no resources held.
 */
public class ProspectingWorkflowImpl implements ProspectingWorkflow {

    // --- Signal state: only set by signal handlers ---
    private boolean paused    = false;
    private boolean aborted   = false;
    private boolean takenOver = false;
    // Queue so rapid-fire signals (EMAIL_REPLY then MEETING_BOOKED) are never lost.
    // Signal handlers enqueue; the workflow loop dequeues one per reasoning cycle.
    private final Queue<String> pendingEvents = new ArrayDeque<>();

    private String status = "RUNNING";

    // --- Activity stub: Temporal generates a proxy that schedules the activity ---
    private final ProspectingActivities activities = Workflow.newActivityStub(
            ProspectingActivities.class,
            ActivityOptions.newBuilder()
                    // How long Temporal waits before retrying if the worker dies mid-activity.
                    // Must be shorter than startToCloseTimeout.
                    .setHeartbeatTimeout(Duration.ofSeconds(30))
                    // Max wall-clock time for a single activity attempt.
                    .setStartToCloseTimeout(Duration.ofMinutes(10))
                    .build()
    );

    // How long to wait for a reply before timing out and re-reasoning.
    private static final Duration REPLY_TIMEOUT    = Duration.ofDays(3);
    // How long to wait for a rep to act on a manual email draft.
    private static final Duration MANUAL_DEADLINE  = Duration.ofDays(1);

    // -------------------------------------------------------------------------
    // @WorkflowMethod — the entry point Temporal calls once per workflow run
    // -------------------------------------------------------------------------
    @Override
    public void run(String workflowId, String goal, String guardrails) {

        // Activities write to DB/external systems; the workflow just orchestrates.
        activities.persistSession(workflowId, goal);

        AgentContext context = new AgentContext(workflowId, goal, guardrails);
        int stepIndex = 0;

        while (true) {

            // Check control signals before spending tokens on a reasoning cycle.
            if (aborted) {
                activities.updateSessionStatus(workflowId, "ABORTED");
                return;
            }
            if (takenOver) {
                activities.updateSessionStatus(workflowId, "HANDED_OVER");
                return;
            }

            // Workflow.await() releases the worker thread until the condition is true.
            // The workflow is "sleeping" — no CPU consumed, no polling.
            // KEY RULE: the lambda must only read workflow-local fields (no I/O).
            Workflow.await(() -> !paused || aborted || takenOver);  // released by resume() signal

            // Sync step progress into context so the agent can reason about where it is.
            context.stepIndex = stepIndex;

            // 1. Run one reasoning cycle — the expensive LLM call.
            FlowAgentResult result = activities.runFlowAgent(context);

            // 2. Persist the result — separate activity so a DB failure retries
            //    only this cheap write, not the entire LLM call above.
            activities.persistReasoningStep(workflowId, stepIndex++, result);

            // 3. Execute the action the agent chose — separate activity so an execution
            //    failure never causes the LLM call to be retried.
            if (result.action() != null
                    && !result.action().equals("WAIT")
                    && !result.action().equals("DONE")) {
                activities.executeAction(workflowId, result.action(), result.detail());
                context.recordTouch();
            }

            switch (result.outcome()) {

                case WAIT -> {
                    boolean signalArrived = Workflow.await(
                            REPLY_TIMEOUT,
                            () -> !pendingEvents.isEmpty() || aborted || takenOver
                    );
                    // Timeout: synthesize event so agent reasons about "no reply yet"
                    context.addEvent(signalArrived ? pendingEvents.poll() : "TIMEOUT");
                }

                case MANUAL_STEP_PENDING -> {
                    boolean done = Workflow.await(
                            MANUAL_DEADLINE,
                            () -> pendingEvents.stream().anyMatch(e -> e.startsWith("STEP_COMPLETED"))
                                    || aborted || takenOver
                    );
                    if (done) {
                        pendingEvents.removeIf(e -> e.startsWith("STEP_COMPLETED"));
                    } else {
                        activities.notifyRepTimeout(workflowId);
                        paused = true;
                    }
                }

                case DONE -> {
                    activities.updateSessionStatus(workflowId, "COMPLETED");
                    status = "COMPLETED";
                    return;
                }

                case ERROR -> {
                    activities.updateSessionStatus(workflowId, "FAILED");
                    activities.notifyRepAgentFailed(workflowId, result.errorMessage());
                    status = "FAILED";
                    return;
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Signal handlers — called by Temporal when an external caller sends a signal.
    // RULE: only flip flags / set a field. Never call activities here.
    // Temporal guarantees these run between activity calls (never mid-activity).
    // -------------------------------------------------------------------------

    @Override public void onEmailReply(String replyBody) { pendingEvents.offer("EMAIL_REPLY:" + replyBody); }
    @Override public void onMeetingBooked(String meetingId) { pendingEvents.offer("MEETING_BOOKED:" + meetingId); }
    @Override public void onStepCompleted(String stepId)  { pendingEvents.offer("STEP_COMPLETED"); }
    @Override public void pause()    { paused    = true;  }
    @Override public void resume()   { paused    = false; }
    @Override public void abort()    { aborted   = true;  }
    @Override public void takeOver() { takenOver = true;  }

    // -------------------------------------------------------------------------
    // Query handler — read-only snapshot. Temporal routes GET requests here.
    // RULE: never modify state inside a query handler.
    // -------------------------------------------------------------------------

    @Override public String getStatus() { return status; }
}
