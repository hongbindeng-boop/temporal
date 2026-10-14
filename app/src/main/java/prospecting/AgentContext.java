package prospecting;

public class AgentContext {
    public String workflowId;
    public String goal;
    public String guardrails;
    public String lastEvent = "START";
    /** How many outreach touches (emails sent, calls made) have occurred so far. */
    public int touchCount = 0;
    /** Current reasoning cycle index (0-based). */
    public int stepIndex  = 0;

    public AgentContext() {}  // required by Jackson for Temporal activity deserialization

    public AgentContext(String workflowId, String goal, String guardrails) {
        this.workflowId = workflowId;
        this.goal = goal;
        this.guardrails = guardrails;
    }

    public void addEvent(String event) { this.lastEvent = event; }
    public void recordTouch()          { this.touchCount++; }
}
