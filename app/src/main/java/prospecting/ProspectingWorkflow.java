package prospecting;

import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.SignalMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface ProspectingWorkflow {

    @WorkflowMethod
    void run(String workflowId, String goal, String guardrails);

    // --- Signals: the only way the outside world wakes this workflow ---

    @SignalMethod
    void onEmailReply(String replyBody);

    @SignalMethod
    void onMeetingBooked(String meetingId);

    @SignalMethod
    void onStepCompleted(String stepId);

    @SignalMethod
    void pause();

    @SignalMethod
    void resume();

    @SignalMethod
    void abort();

    @SignalMethod
    void takeOver();

    // --- Queries: read-only snapshot, never modifies state ---

    @QueryMethod
    String getStatus();
}
