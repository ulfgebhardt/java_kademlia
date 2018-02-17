package routingtable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import node.Identifier;
import node.Node;
import node.NodeIdentifier;

public class RoutingTableImpl implements IRoutingTable {
	private Set<NodeIdentifier> entries = new HashSet<NodeIdentifier>();

	private Bucket root;

	private int bucketSize;

	public RoutingTableImpl(int bucketSize, Node node) {
		this.bucketSize = bucketSize;
		this.root = new Bucket(bucketSize, 0, node);
	}

	@Override
	public void insert(NodeIdentifier id) {
		if (root.contains(id)) {
			root.update(id);
		} else {
			entries.add(id);
			root.insert(id);
		}
	}

	@Override
	public Set<NodeIdentifier> getClosestNodesTo(final Identifier id) {
		Set<NodeIdentifier> result = new HashSet<NodeIdentifier>();

		if (entries.size() <= bucketSize) {
			result.addAll(entries);

		} else {
			List<NodeIdentifier> temp = new ArrayList<NodeIdentifier>(entries);

			Collections.sort(temp, new Comparator<NodeIdentifier>() {
				@Override
				public int compare(NodeIdentifier o1, NodeIdentifier o2) {
					BigInteger dist1 = id.distanceTo(o1);
					BigInteger dist2 = id.distanceTo(o2);
					return dist1.compareTo(dist2);
				}
			});

			for (int i = 0; i < bucketSize; i++) {
				result.add(temp.get(i));
			}
			result = new HashSet<NodeIdentifier>(temp.subList(0,
					Node.BUCKET_SIZE));
		}
		return result;
	}

	@Override
	public boolean contains(NodeIdentifier node) {
		return root.contains(node);
	}

	@Override
	public void remove(NodeIdentifier node) {

	}

	@Override
	public Set<NodeIdentifier> getEntries() {
		return entries;
	}
}