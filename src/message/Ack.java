package message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import node.Identifier;
import node.NodeIdentifier;
import util.BufferUtil;

public class Ack {
    private final static Logger LOGGER = Logger.getLogger(Ack.class.getName());

    /**
     * timeout in seconds
     */
    private static final int TIMEOUT = 1000;

    /**
     * Maximum number of retries
     */
    private static final int MAX_RETRIES = 3;

    private Identifier rpcId;

    private NodeIdentifier receiver;

    private ByteBuffer buffer;

    private int numRetries = 0;

    private TimeoutThread timeout;
    private Thread thread;

    // The channel to re-send the message on
    private DatagramChannel channel;

    private MessageCallback callback;

    public Ack(Identifier id, NodeIdentifier receiver, DatagramChannel channel,
            ByteBuffer buffer, MessageCallback cb) {
        this.rpcId = id;
        this.receiver = receiver;
        this.channel = channel;
        this.buffer = BufferUtil.clone(buffer);
        this.callback = cb;
        startThread();
    }

    private void startThread() {
        LOGGER.log(Level.FINEST, "Starting timeout thread for RPC " + rpcId);
        timeout = new TimeoutThread();
        thread = new Thread(timeout);
        thread.start();
    }

    public Identifier getID() {
        return rpcId;
    }

    public boolean check(NodeIdentifier fromID) {
        return fromID.equals(receiver);
    }

    public ByteBuffer getBuf() {
        return buffer;
    }

    public void setBuf(ByteBuffer buf) {
        this.buffer = buf;
    }

    public void setReceived() {
        // Stop thread
        try {
            if (thread != null) {
                timeout.terminate();
                thread.join();
            }
        } catch (InterruptedException e) {
        }
    }

    private class TimeoutThread implements Runnable {
        private volatile boolean notReceived = true;

        // When do we stop expecting the ack
        private long timeToStop = System.currentTimeMillis() + TIMEOUT;

        @Override
        public void run() {
            while (notReceived && System.currentTimeMillis() < timeToStop) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // Timeout hit!

            if (notReceived) {

                if (numRetries < MAX_RETRIES) {
                    try {
                        LOGGER.log(
                                Level.FINE,
                                "Didn't receive RPC Ack {0} by now. Resending... ",
                                new Object[] { rpcId });
                        LOGGER.log(Level.INFO, receiver.getAddress().toString());                        
                        channel.send(buffer, receiver.getAddress());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    startThread();
                    numRetries++;
                } else {

                    LOGGER.log(Level.INFO, "Absent RPC ack {0}.",
                            new Object[] { rpcId });

                    if (callback != null) {
                        callback.onTimeout();
                    }
                }
            } else {
                // Message has been received in time
                if (callback != null) {
                    callback.onReceive();
                }
            }
        }

        public void terminate() {
            notReceived = false;
        }
    }
}