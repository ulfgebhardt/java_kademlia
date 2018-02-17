package node;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import message.Ack;
import message.MessageCallback;
import message.MessageType;
import routingtable.IRoutingTable;
import routingtable.RoutingTableImpl;

public class Node {

	private final static Logger LOGGER = Logger.getLogger(Node.class.getName());

	/**
	 * Size of ID space (has to be a multiple of 8)
	 */
	public static final int ID_BITS = 8;
	/**
	 * The bucket size
	 */
	public static final int BUCKET_SIZE = 2;
	/**
	 * The first node is always spawned on port 50000
	 */
	private static final int INITIAL_PORT = 50000;
	private static final Identifier INITIAL_ID = Identifier
			.getStaticIdentifier(ID_BITS);
	private static final int BUFFER_SIZE = 512;
        private static final int CHUNK_SIZE = 4;
	/**
	 * The size of an IP address (in bytes)
	 */
	public static final int SIZE_IP_ADDRESS = 8;

	private InetSocketAddress address;
	private DatagramChannel channel;

        public NodeIdentifier lastlookup = null;

	private Map<Identifier, List<Ack>> rpcs = new HashMap<Identifier, List<Ack>>();
	private Map<Identifier, Identifier> values = new HashMap<Identifier, Identifier>();

	private Identifier searchID = null;

	private Thread thread;
	private UDPHandler udpListen;

	private Identifier nodeID = Identifier.getRandomIdentifier(ID_BITS);
	private IRoutingTable routingTable = new RoutingTableImpl(BUCKET_SIZE, this);

	private Map<FileIdentifier, String> data = new HashMap<FileIdentifier, String>();;

	public Node() {
		System.setProperty("java.net.preferIPv4Stack", "true");

		try {
			channel = DatagramChannel.open();

			try {
				address = new InetSocketAddress("localhost", INITIAL_PORT);
				channel.socket().bind(address);

				this.nodeID = INITIAL_ID;
			} catch (SocketException e) {
				// The initial port is already bound -> let the system pick a
				// port
				channel.socket().bind(new InetSocketAddress("localhost", 0));
				address = (InetSocketAddress) channel.getLocalAddress();
			}

			channel.configureBlocking(false);

			udpListen = new UDPHandler(this);
			thread = new Thread(udpListen);
			thread.start();

			LOGGER.log(Level.INFO, "{0}: Initialized node {1} on {2}",
					new Object[] { this.nodeID, getName(), address.toString() });

			if (address.getPort() != INITIAL_PORT) {
				// The port of this node is not the "INITIAL_PORT" (so it's not
				// the first node in the network). So we try to join the network
				// via the first node.
				NodeIdentifier viaNode = new NodeIdentifier(ID_BITS,
						INITIAL_ID.getBytes(), new InetSocketAddress(
								"127.0.0.1", INITIAL_PORT));
				joinNetworkVia(viaNode);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void joinNetworkVia(NodeIdentifier viaNode) {
		LOGGER.log(Level.INFO, "Trying to join network via node {0}",
				new Object[] { viaNode });

		routingTable.insert(viaNode);
		sendFindNode(viaNode, this.nodeID);
	}

	/**
	 * Creates and returns new ID (usually used as a RPC ID). This makes sure
	 * the ID is not yet used (in this node).
	 * 
	 * @return an ID
	 */
	private Identifier createRPCID() {
		Identifier rpcID = Identifier.getRandomIdentifier(ID_BITS);
		while (rpcs.containsKey(rpcID)) {
			rpcID = Identifier.getRandomIdentifier(ID_BITS);
		}
		return rpcID;
	}

	void sendFindNode(NodeIdentifier receiver, Identifier idToFind) {
		boolean successful = send(receiver, MessageType.FIND_NODE,
				idToFind.getBytes(), true, null);

		if (successful) {
			LOGGER.log(Level.INFO, "Sending [FIND_NODE {0}] to node {1}",
					new Object[] { idToFind, receiver });
		}
	}

	void sendFindValue(NodeIdentifier receiver, Identifier idToFind) {                
		// need to save the fileID because we need it for future searches
		this.searchID = idToFind;

                LOGGER.log(Level.INFO, "Sending [FIND_VALUE {0}] to node {1}",new Object[] { idToFind, receiver });
		boolean successful = send(receiver, MessageType.FIND_VALUE,
				idToFind.getBytes(), true, null);

		if (successful) {
			LOGGER.log(Level.INFO, "Sent [FIND_VALUE {0}] to node {1}",
					new Object[] { idToFind, receiver });
		}
	}

	void sendFoundValue(NodeIdentifier receiver, Identifier idToFind,
			Identifier rpcID) {
		boolean successful = send(receiver, MessageType.FOUND_VALUE, rpcID,
				values.get(idToFind).getBytes(), false, null);

		if (successful) {
			LOGGER.log(Level.INFO, "Sending [FOUND_VALUE {0} -> {1}] to node {2}",
					new Object[] { idToFind, values.get(idToFind), receiver });
		}
	}

	/**
	 * Gets all nodes of this nodes routing table, that are close to a given
	 * node/fileID and sends that list to a specific node.
	 * 
	 * @param receiver
	 *            The node to receive the list of nodes
	 * @param idToFind
	 *            The ID to find close nodes of
	 * @param rpcID
	 *            An RPC ID (because this is always an answer to a FIND_NODE
	 *            RPC)
	 * @param nodeType
	 *            If true, we search a specific node, else a fileID
	 */
	void sendClosestNodesTo(NodeIdentifier receiver, Identifier idToFind,
			Identifier rpcID, boolean nodeType) {
		byte msgtype = 0;
		if (nodeType) {
			msgtype = MessageType.NODES;
		} else {
			msgtype = MessageType.VALUE_NODES;
		}

		Set<NodeIdentifier> closeNodes = routingTable
				.getClosestNodesTo(idToFind);
		int numNodes = closeNodes.size();

		ByteBuffer nodes = ByteBuffer.allocate(numNodes * (ID_BITS / 8)
				+ numNodes * SIZE_IP_ADDRESS);

		for (NodeIdentifier idToSend : closeNodes) {
			// Don't send the node to itself
			if (!receiver.equals(idToSend)) {
				nodes.put(idToSend.getTripleAsBytes());
			}
		}

		boolean successful = send(receiver, msgtype, rpcID, nodes.array(),
				false, null);

		if (successful) {
			LOGGER.log(
					Level.INFO,
					"Sending {0} nodes to to node {1} [FIND_NODE {2}] (rpcID={3})",
					new Object[] { closeNodes.size(), receiver, idToFind, rpcID });
		}
	}

	public void sendStore(NodeIdentifier receiver, Identifier fileID) {
		boolean successful = send(receiver, MessageType.STORE,
				fileID.getBytes(), true, null);

		if (successful) {
			LOGGER.log(Level.INFO, "Sending [STORE {0}] to node {1}",
					new Object[] { fileID, receiver });
		}
	}

	public void sendAck(NodeIdentifier receiver, Identifier rpcID) {
		send(receiver, MessageType.ACK, rpcID, null, false, null);
	}

        public void sendDataReq(FileIdentifier fileID){
            //TODO
            if(lastlookup == null){
                new Exception("lookup first!").printStackTrace();
                return;}
            //String id = "128";
            //NodeIdentifier receiver = new NodeIdentifier(8, id.getBytes(), new InetSocketAddress("localhost", INITIAL_PORT));
            send(lastlookup, MessageType.DATA_REQ, fileID.getBytes(), true, null);
        }

	public void sendData(NodeIdentifier receiver, Identifier fileID) {            
                
                String data = this.data.get(fileID);
                if(data == null){
                    //TODO We dont have that data. -> DOES NOT WORK PROPERLY!
                    new Exception().printStackTrace();
                    return;
                }
                int CHUNK_COUNT = data.length()/CHUNK_SIZE;

                for(int i = 0; i<CHUNK_COUNT; i++){
                    String chunk =  fileID.toString() + "-" +
                                    CHUNK_COUNT + "-" +
                                    i + "-" +
                                    data.substring(i*CHUNK_SIZE, (i+1)*CHUNK_SIZE);
                    send(receiver, MessageType.DATA, chunk.getBytes(), true, null);
                }		
	}

	public void sendPing(NodeIdentifier receiver, MessageCallback cb) {
		boolean successful = send(receiver, MessageType.PING, null, true, cb);

		if (successful) {
			LOGGER.log(Level.INFO, "Sending [PING] to node {0}",
					new Object[] { receiver });
		}
	}

	void sendPong(NodeIdentifier receiver, Identifier rpcID) {
		boolean successful = send(receiver, MessageType.PONG, rpcID, null,
				false, null);

		if (successful) {
			LOGGER.log(Level.INFO, "Sending [PONG] to {0} (rpcID={1})",
					new Object[] { receiver, rpcID });
		}
	}

	/**
	 * Send a message to a given ID (with a given RPC ID). You usually want to
	 * use this method when you know the RPC ID beforehand (e.g. if this is an
	 * ack or answer to a prior message).
	 * 
	 * @param to
	 *            the ID to send to
	 * @param messageType
	 *            the message type
	 * @param data
	 *            the data to send
	 * @param reliable
	 *            flag, whether this has to be acked or not
	 * @param cb
	 *            A callback that is executed when this message gets acked (or
	 *            answered). This obviously is only of interest when the
	 *            reliable flag is true
	 * @return true if the message was sent successfully
	 */
	private boolean send(NodeIdentifier to, byte messageType, byte[] data,
			boolean reliable, MessageCallback cb) {
		return send(to, messageType, createRPCID(), data, reliable, cb);
	}

	/**
	 * Send a message to a given ID (with a given RPC ID). You usually want to
	 * use this method when you know the RPC ID beforehand (e.g. if this is an
	 * ack or answer to a prior message).
	 * 
	 * @param to
	 *            the ID to send to
	 * @param messageType
	 *            the message type
	 * @param rpcID
	 *            the RPC ID of this message (if you don't know this use
	 *            {@link #send(NodeIdentifier, byte, byte[], boolean, MessageCallback)}
	 *            and a new random ID will be created)
	 * @param data
	 *            the data to send
	 * @param reliable
	 *            flag, whether this has to be acked or not
	 * @param cb
	 *            A callback that is executed when this message gets acked (or
	 *            answered). This obviously is only of interest when the
	 *            reliable flag is true
	 * @return true if the message was sent successfully
	 */
	private boolean send(NodeIdentifier to, byte messageType, Identifier rpcID,
			byte[] data, boolean reliable, MessageCallback cb) {
                
		boolean successful = true;
		ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);


		buffer.put(messageType);
		buffer.put(this.nodeID.getBytes());
		buffer.put(rpcID.getBytes());

		if (data != null) {
			buffer.put(data);
		}

                LOGGER.log(Level.INFO, "Sending Data to:{0} with MT:"+messageType+" data:"+buffer.toString(), to.getAddress().toString());

		buffer.flip();

		try {

			channel.send(buffer, to.getAddress());

		} catch (IOException e) {

			LOGGER.log(Level.SEVERE, "Failed to write to channel. To:"+to.getAddress().toString()+" buffer: "+buffer.toString(), e);
			successful = false;

		} finally {
			// Even if an exception occurred this should be reliable
			if (reliable) {

				Ack newAck = new Ack(rpcID, to, channel, buffer, cb);
				if (rpcs.containsKey(rpcID)) {
					rpcs.get(rpcID).add(newAck);
				} else {
					rpcs.put(rpcID, new ArrayList<Ack>());
					rpcs.get(rpcID).add(newAck);
				}
			}
		}
		return successful;
	}

	public String getName() {
		return nodeID.toString();
	}

	public boolean hasAcks() {
		return !rpcs.isEmpty();
	}

	public DatagramChannel getChannel() {
		return channel;
	}

	public void updateBuckets(NodeIdentifier id) {
		routingTable.insert(id);
	}

	public Identifier getID() {
		return nodeID;
	}

	public Set<NodeIdentifier> getNeighbors() {
		return routingTable.getEntries();
	}

	public void storePair(Identifier key, Identifier nodeid) {
		values.put(key, nodeid);
	}

	public void store(Identifier key) {
		
		storePair(key,this.nodeID);
		
		Set<NodeIdentifier> nodes = routingTable.getClosestNodesTo(key);

		
		for (NodeIdentifier node : nodes) {
			sendStore(node, key);
		}
	}

	public void findValue(Identifier key) {
		Set<NodeIdentifier> nodes = routingTable.getClosestNodesTo(key);

		
		for (NodeIdentifier node : nodes) {                        
			sendFindValue(node, key);                        
		}
	}

	public boolean hasKey(Identifier key) {
		return values.containsKey(key);
	}

	public Identifier getSearchID() {
		return this.searchID;
	}

	public boolean receivedRPC(NodeIdentifier fromID, Identifier rpcID) {
		List<Ack> rpcsFromID = rpcs.get(rpcID);
		boolean removedAck = false;
				
		// wohl unschön, hier auf != null zu prüfen, da Fehler wo anders ist.
		if (rpcsFromID != null) {
			for (Ack ack : rpcsFromID) {
				if (ack.check(fromID)) {
					ack.setReceived();
					rpcsFromID.remove(ack);
					removedAck = true;

					LOGGER.log(Level.FINEST, "Received RPC ack " + rpcID);

					break;
				}
			}
		}

		if (!removedAck) {
			LOGGER.log(Level.WARNING,
					"Received RPC ack {0}, but didn't expect that",
					new Object[] { rpcID });
		}

		return removedAck;
	}

	public void leave() {
		for (NodeIdentifier n : getNeighbors()) {
			sendLeave(n);
		}
		System.exit(0);
	}

	private boolean sendLeave(NodeIdentifier n) {
		return send(n, MessageType.LEAVE, null, false, null);
	}

	public void storeData(FileIdentifier id, String data) {
		this.data.put(id, data);
		LOGGER.log(Level.INFO, "Stored Data [{0}] as [{1}])",
				new Object[] { data, id});
	}
	

	public void sendFile(NodeIdentifier nodeID, File file) {

		
	}
}