package routingtable;

import java.util.Set;

import node.Identifier;
import node.NodeIdentifier;

public interface IRoutingTable {

    public void insert(NodeIdentifier id);

    public Set<NodeIdentifier> getClosestNodesTo(Identifier id);

    public boolean contains(NodeIdentifier node);

    public void remove(NodeIdentifier node);

    public Set<NodeIdentifier> getEntries();
}