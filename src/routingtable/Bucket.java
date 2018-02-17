package routingtable;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import message.MessageCallback;
import node.Node;
import node.NodeIdentifier;

public class Bucket {
    private final static Logger LOGGER = Logger.getLogger(Bucket.class
            .getName());

    private Bucket left;
    private Bucket right;

    private List<NodeIdentifier> entries;

    private int bucketSize;
    private int level;

    private Node node;

    public Bucket(int bucketSize, int level, Node node) {
        this.bucketSize = bucketSize;
        this.level = level;
        this.node = node;
        entries = new ArrayList<NodeIdentifier>();
    }

    /**
     * Returns the nodes of this very bucket.
     * 
     * @return
     */
    public List<NodeIdentifier> getNodes() {
        return entries;
    }

    public boolean contains(NodeIdentifier id) {
        if (!isLeaf()) {
            return left.contains(id) || right.contains(id);
        }
        return entries.contains(id);
    }

    /**
     * Tries to update the given node.
     * 
     * @param id
     * @return true if the node is still available, else false
     */
    public void update(final NodeIdentifier id) {
        if (!isLeaf()) {
            if (id.isBitSetAt(level)) {
                left.update(id);
            } else {
                right.update(id);
            }
        } else {
            node.sendPing(id, new MessageCallback() {
                @Override
                public void onReceive() {
                    LOGGER.log(Level.INFO,
                            "Node answered in time, moving to top of list.");
                    entries.remove(id);
                    entries.add(0, id);
                }

                @Override
                public void onTimeout() {
                    LOGGER.log(Level.INFO, "Node didnt answer in time.");
                    // TODO: this should be propagated to the "upper" Routing
                    // Table, not just to this specific bucket
                    entries.remove(id);
                }
            });
        }
    }

    public void insert(NodeIdentifier newId) {
        insert(newId, "");
    }

    public void insert(NodeIdentifier newId, String path) {
        if (isLeaf()) {
            if (entries.size() < bucketSize) {
                LOGGER.log(Level.INFO,
                        "Added node {0} to RT [{1}] on level {2}",
                        new Object[] { newId, path, level });
                entries.add(newId);
            } else {
                LOGGER.log(Level.INFO, "Split on level " + level
                        + " while adding " + newId);

                LOGGER.log(Level.INFO,
                        "Distributing present nodes to lower buckets");

                Bucket newLeft = new Bucket(bucketSize, level + 1, node);
                Bucket newRight = new Bucket(bucketSize, level + 1, node);

                // Add the new entry and in the following loop distribute all
                // existing entries to left/right
                entries.add(newId);

                for (NodeIdentifier id : entries) {
                    if (id.isBitSetAt(level)) {
                        newLeft.insert(id, path + "1");
                    } else {
                        newRight.insert(id, path + "0");
                    }
                }

                this.entries = null;
                this.left = newLeft;
                this.right = newRight;
            }
        } else {
            if (newId.isBitSetAt(level)) {
                left.insert(newId, path + "1");
            } else {
                right.insert(newId, path + "0");
            }
        }

    }

    private boolean isLeaf() {
        return left == null && right == null;
    }

    public void remove(NodeIdentifier node) {
        if (isLeaf()) {
            entries.remove(node);
        } else {
            if (node.isBitSetAt(level)) {
                left.remove(node);
            } else {
                right.remove(node);
            }
        }
    }
}