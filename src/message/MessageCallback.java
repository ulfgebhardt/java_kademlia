package message;

/**
 * A callback to create asynchronous events that get triggered when a message
 * (ack/answer) is received.
 * 
 * @author jln
 * 
 */
public interface MessageCallback {

    /**
     * Called when the awaited message arrives.
     */
    public void onReceive();

    /**
     * Called when the awaited message doesn't arrive (even after possible
     * retries).
     */
    public void onTimeout();
}
