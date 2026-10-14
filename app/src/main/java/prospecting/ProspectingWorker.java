package prospecting;

import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;

/**
 * Run this first — it connects to localhost:7233 and polls the task queue.
 * Keep it running while you test.
 */
public class ProspectingWorker {

    public static final String TASK_QUEUE = "prospecting-queue";

    public static void main(String[] args) {
        // Connect to local Temporal server (default port 7233, not the UI port 8080)
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);
        WorkerFactory factory = WorkerFactory.newInstance(client);

        Worker worker = factory.newWorker(TASK_QUEUE);

        // Register the workflow implementation.
        // Temporal creates a new instance per workflow run (never reuses state).
        worker.registerWorkflowImplementationTypes(ProspectingWorkflowImpl.class);

        // Register the activity implementation (shared across runs — must be thread-safe).
        worker.registerActivitiesImplementations(new ProspectingActivitiesImpl());

        System.out.println("Worker started on task queue: " + TASK_QUEUE);
        factory.start();  // blocks; polls Temporal for work
    }
}
