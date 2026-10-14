# temporal

A small proof of concept that shows:

1. [How to run Temporal locally](#1-run-temporal-locally)
2. [What Temporal is](#2-what-is-temporal)
3. [How `ProspectingWorkflowImpl` uses Temporal](#3-how-prospectingworkflowimpl-adapts-to-temporal)

## 1. Run Temporal locally

Prerequisites: Docker with Compose, JDK 17+, Maven. The Maven project lives in `app/`. Maven downloads all dependencies from Maven Central, so nothing else needs to be checked in. The jars in `lib/` are git-ignored and not needed.

### Start the Temporal server

```bash
git clone https://github.com/temporalio/docker-compose.git
cd docker-compose
docker compose up
```

When it is up:

| What | Where |
|------|-------|
| Temporal server (gRPC, used by the worker and starter) | `localhost:7233` |
| Temporal Web UI | http://localhost:8080 |

### Run the sample

`runFlowAgent` calls Gemini (`gemini-2.5-flash`) through Google ADK on Vertex AI. Set these environment variables in the terminal that runs the worker, and authenticate with `gcloud auth application-default login`. Project and location:

```bash
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT=internal-468309
export GOOGLE_CLOUD_LOCATION=us-east4
```

If the call fails, the workflow ends in `FAILED`, so the Temporal flow is still visible.

Use two terminals, starting from the root of this repo (clone it into a separate directory from `docker-compose`):

```bash
# Terminal 1: worker. Keep it running; it polls the "prospecting-queue" task queue.
cd app
mvn compile exec:java -Dexec.mainClass=prospecting.ProspectingWorker

# Terminal 2: starter. It starts one workflow and prints its ID.
cd app
mvn compile exec:java -Dexec.mainClass=prospecting.ProspectingStarter
```

You can also run the two `main` methods from your IDE.

Then open http://localhost:8080, find the workflow `prospecting-contact-<timestamp>`, and open its **History** tab to see each activity and signal as an event. The worker terminal prints `[DB]`, `[ADK]`, `[ACTION]` and `[NOTIFY]` lines for each activity. These are stubs; only the Gemini call is real.

### Send signals and query status

The signal examples in `ProspectingStarter` are commented out. Uncomment them, or use the Temporal CLI (use the workflow ID the starter printed):

```bash
temporal workflow signal --workflow-id <id> --name onEmailReply --input '"Hi, interested!"'
temporal workflow query  --workflow-id <id> --type getStatus
```

## 2. What is Temporal?

Temporal is a durable execution engine. You write a long-running business process as ordinary code (a **workflow**). Temporal records every step in an event history, so the process survives worker crashes, deploys and multi-day waits.

| Concept | Meaning |
|---------|---------|
| **Workflow** | Orchestration code. It must be deterministic: no I/O, no random values, no wall-clock time. Temporal can replay it from history to rebuild its state. |
| **Activity** | Any side effect: DB write, HTTP call, LLM call. Temporal runs it on a worker and retries it with timeouts. |
| **Worker** | Your process. It polls a **task queue** and runs workflow and activity code. |
| **Task queue** | The named queue that connects clients to workers (`prospecting-queue` here). |
| **Signal** | An asynchronous message that sends an event into a running workflow. |
| **Query** | A read-only view of a workflow's current state. |
| **Temporal server** | Stores the event histories and schedules tasks. It runs your code only through workers. |

```
 Starter / API ──start, signal, query──▶ Temporal server ◀──poll── Worker
                                         (history, timers,          ├─ workflow code (deterministic)
                                          task queues)              └─ activity code (side effects)
```

Why it is useful: the workflow can sleep for 3 days with `Workflow.await(...)` without holding a thread. If the worker restarts, Temporal replays the history and the workflow continues where it left off. Retries and timeouts don't need hand-written code.

Further reading: https://docs.temporal.io/

## 3. How `ProspectingWorkflowImpl` adapts to Temporal

The use case is an AI sales-prospecting agent. Given a goal ("Book a demo meeting") and guardrails ("Email only, max 3 touches"), it repeatedly decides what to do next, such as sending an email or waiting for a reply. It continues until the goal is met or it is stopped.

### Files

| File | Role |
|------|------|
| `ProspectingWorkflow` | Workflow interface: one `@WorkflowMethod`, signals, one query |
| `ProspectingWorkflowImpl` | The deterministic orchestration loop |
| `ProspectingActivities` / `ProspectingActivitiesImpl` | All side effects (stubs, plus the real LLM call) |
| `FlowAgentRunner` | Google ADK / Gemini reasoning step, called from the `runFlowAgent` activity |
| `ProspectingWorker` | Registers the workflow and activities and polls `prospecting-queue` |
| `ProspectingStarter` | Starts a workflow and queries it |

### The loop

```
persistSession
loop:
  aborted / takenOver?           → updateSessionStatus, return
  await (not paused)             ← resume() signal
  runFlowAgent        (activity) ← the LLM decides the next action
  persistReasoningStep(activity) ← separate so a DB failure never re-runs the LLM
  executeAction       (activity) ← separate so an email failure never re-runs the LLM
  switch outcome:
    WAIT                → await signal or 3-day timeout (timeout becomes a "TIMEOUT" event)
    MANUAL_STEP_PENDING → await onStepCompleted or 1 day; on timeout notify the rep and pause
    DONE                → status COMPLETED, return
    ERROR               → status FAILED, notify the rep, return
```

### Where each Temporal concept shows up

| Concept | In `ProspectingWorkflowImpl` |
|---------|------------------------------|
| **Activities for every side effect** | The class does no DB, HTTP or parsing. It calls `activities.*` through a stub from `Workflow.newActivityStub`, with `StartToCloseTimeout` 10 min and `HeartbeatTimeout` 30 s. |
| **Splitting activities by failure cost** | The expensive LLM call (`runFlowAgent`) is separate from the cheap DB write and from the action execution. A retry repeats only the step that failed. |
| **Durable sleep** | `Workflow.await(REPLY_TIMEOUT, ...)` waits up to 3 days for a reply. No thread is held and no polling happens. |
| **Signals** | `onEmailReply`, `onMeetingBooked` and `onStepCompleted` push events into a `Queue`. `pause`, `resume`, `abort` and `takeOver` flip boolean flags. Handlers only change fields and never call activities. |
| **Event queue** | Rapid-fire signals (reply, then meeting booked) are queued so none is lost. The loop handles one per reasoning cycle. |
| **Query** | `getStatus()` returns the current status without changing state. |
| **Determinism** | The only state is plain fields, mutated by signals and by deterministic workflow code. The `await` lambdas read only workflow fields. |
| **Worker setup** | `ProspectingWorker` registers `ProspectingWorkflowImpl.class` (a new instance per run) and one shared `ProspectingActivitiesImpl` (must be thread-safe). |
| **Starting** | `ProspectingStarter` sets an explicit workflow ID and a 90-day execution timeout, longer than the longest wait. It uses `WorkflowClient.start` to start asynchronously. |

### Ideas to try

- Kill the worker mid-run and restart it. The workflow resumes from its history.
- Send `pause`, then `resume`, and watch the loop block and release at `Workflow.await`.
- Send `abort` or `takeOver` and watch the workflow end with `ABORTED` or `HANDED_OVER`.
- Make an activity throw an exception and watch Temporal retry it in the UI.

### Known POC limits

- Reasoning history is kept in memory in `ProspectingActivitiesImpl` and is lost on a worker restart. Production would read it from Postgres inside the activity.
- DB, email and notification activities only print to the console.
