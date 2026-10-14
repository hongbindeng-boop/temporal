package prospecting;

/**
 * @param outcome      Temporal routing: what the workflow loop does next.
 * @param action       What the agent decided: SEND_EMAIL | MAKE_CALL | WAIT | DONE.
 * @param detail       Payload (email subject+body, call plan); empty for WAIT/DONE.
 * @param reasoning    Agent's chain-of-thought — why this step advances the goal.
 * @param errorMessage Non-null only when outcome == ERROR.
 */
public record FlowAgentResult(Outcome outcome, String action, String detail, String reasoning, String errorMessage) {}
