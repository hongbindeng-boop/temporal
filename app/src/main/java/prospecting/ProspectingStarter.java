package prospecting;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.client.WorkflowStub;
import io.temporal.serviceclient.WorkflowServiceStubs;

import java.time.Duration;

/**
 * Run this to start a workflow, send signals, and query state.
 * The worker must already be running.
 *
 * Maps to section 9 (Control API) of the design doc.
 */
public class ProspectingStarter {

    public static void main(String[] args) throws InterruptedException {
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);

        String workflowId = "prospecting-contact-" + System.currentTimeMillis();

        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowId(workflowId)                    // deterministic ID — idempotent start
                .setTaskQueue(ProspectingWorker.TASK_QUEUE)
                .setWorkflowExecutionTimeout(Duration.ofDays(90))  // must exceed max wait (design doc §11)
                .build();

        ProspectingWorkflow workflow = client.newWorkflowStub(ProspectingWorkflow.class, options);

        // Start async — WorkflowClient.start() returns immediately, workflow runs on worker.
        WorkflowClient.start(workflow::run, workflowId, "Book a demo meeting", "Email only, max 3 touches");
        System.out.println("Workflow started: " + workflowId);
        System.out.println("UI: http://localhost:8080/namespaces/default/workflows/" + workflowId);

        // --- Query: read status without interrupting the workflow ---
        // Use a typed stub to call @QueryMethod
        ProspectingWorkflow runningWorkflow = client.newWorkflowStub(ProspectingWorkflow.class, workflowId);
        System.out.println("Status: " + runningWorkflow.getStatus());

//        // --- Send signals: wake the workflow with external events ---
//        // (In prod these come from Redis reply-correlation lookup.)
//        Thread.sleep(2000);
//        System.out.println("Sending EMAIL_REPLY signal...");
//        runningWorkflow.onEmailReply("Hi, interested! Can we chat?");
//
//        Thread.sleep(2000);
//        System.out.println("Sending MEETING_BOOKED signal...");
//        runningWorkflow.onMeetingBooked("meeting-xyz-999");
//
//        // --- Untyped stub: useful when you only know the workflowId (e.g. from Redis) ---
//        // This is what your reply-ingest handler would use in prod.
//        WorkflowStub untypedStub = client.newUntypedWorkflowStub(workflowId);
//        untypedStub.signal("onEmailReply", "Another reply via untyped stub");

        System.out.println("Done — watch the worker terminal for activity output.");
        System.out.println("Open http://localhost:8080 to see the workflow in the Temporal UI.");
    }
}
