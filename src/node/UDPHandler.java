package node;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import message.MessageType;

public class UDPHandler implements Runnable {
	private final static Logger LOGGER = Logger.getLogger(UDPHandler.class
			.getName());

	public static final int BUF_SIZE = 512;

	private volatile boolean running = true;
	private ByteBuffer buffer = ByteBuffer.allocate(BUF_SIZE);

	private Node node;

        HashMap<FileIdentifier, HashMap<Integer,String> > chunklist = new HashMap<FileIdentifier, HashMap<Integer, String> >();
        
	public UDPHandler(Node node) {
		this.node = node;
	}

	/**
	 * Takes the buffer of this UDPHandler as is and tries to read an IP address
	 * (4 bytes and 1 int) from it. If there is no/incomplete or wrong data,
	 * this will fail.
	 * 
	 * @return the address that has been read
	 */
	private InetSocketAddress getIPFromBuffer() {
		StringBuilder theAddr = new StringBuilder();
		// Read 4 Bytes and 1 Integer = 1 IP address
		for (int i = 0; i < 4; i++) {
			theAddr.append(buffer.get());
			if (i < 3) {
				theAddr.append(".");
			}
		}                
		int port = buffer.getInt();
		return new InetSocketAddress(theAddr.toString(), port);
	}

	private Identifier getIDFromBuffer() {
		int numBytes = Node.ID_BITS / 8;
		byte[] result = new byte[numBytes];
		for (int i = 0; i < numBytes; i++) {
			result[i] = buffer.get();
		}
		return new Identifier(Node.ID_BITS, result);
	}        

	/**
	 * Reads a triple <IP address, port, id> from the channel and returns a
	 * {@link node.NodeIdentifier}.
	 * 
	 * @return the read node ID
	 */
	private NodeIdentifier getNodeTripleFromBuffer() {
		InetSocketAddress address = getIPFromBuffer();

		int numBytes = Node.ID_BITS / 8;
		byte[] result = new byte[numBytes];
		for (int i = 0; i < numBytes; i++) {
			result[i] = buffer.get();
		}                
                LOGGER.log(Level.INFO,"Read Buffer id: "+Node.ID_BITS+" result: "+result+" addr: "+address);
		return new NodeIdentifier(Node.ID_BITS, result, address);
	}

	public void run() {
		InetSocketAddress from = null;

		// Run until it gets killed, and all my Acks have been answered
		while (running || node.hasAcks()) {
			try {
				// Flag that indicates whether the routing table should be
				// updated with the node we just received a message from. This
				// needs to be done, because some messages trigger a direct
				// answer. For example we send a PING to a node. That node
				// answers with a PONG. Because we received a message from that
				// node we will update our routing table and see that we already
				// know this node. So we will PING that node...
				boolean updateRT = true;

				// The address of the node that sent this message
				from = (InetSocketAddress) node.getChannel().receive(buffer);

				// channel.receive() is non-blocking. So we need to check if
				// something actually has been written to the buffer
				if (buffer.remaining() != BUF_SIZE) {
					buffer.flip();

					byte messageType = buffer.get();

					NodeIdentifier fromID = new NodeIdentifier(Node.ID_BITS,
							getIDFromBuffer().getBytes(), from);

					Identifier rpcID = getIDFromBuffer();

					switch (messageType) {
					case MessageType.FIND_NODE:
						receiveFindNode(fromID, rpcID);
						break;
					case MessageType.NODES:
						receiveNodes(fromID, rpcID);
						break;
					case MessageType.PING:
						updateRT = false;
						receivePing(fromID, rpcID);
						break;
					case MessageType.PONG:
						updateRT = false;
						receivePong(fromID, rpcID);
						break;
					case MessageType.LEAVE:
						// We don't have to do anything here because, after this
						// switch block we call node.updateBuckets(...) which
						// will try to ping the node we received this leave
						// message from. That node will not answered because it
						// directly shut down after sending the leave message.
						// So the node will be removed from this routing table.
						LOGGER.log(Level.INFO, "Received leave from {0}",
								new Object[] { from.toString() });
						break;
					case MessageType.FIND_VALUE:
						receiveFindValue(fromID, rpcID);
						break;
					case MessageType.VALUE_NODES:
						receiveValueNodes(fromID, rpcID);
						break;
					case MessageType.FOUND_VALUE:
						receiveFoundValue(fromID, rpcID);
						break;
					case MessageType.STORE:
						receiveStore(fromID, rpcID);
						break;
					case MessageType.DATA:
						receiveData(fromID, rpcID);
						LOGGER.log(Level.INFO, "Received DATA from {0}",
								new Object[] { from.toString() });
						break;
                                        case MessageType.DATA_REQ:
                                                receiveDataReq(fromID,rpcID);
                                                LOGGER.log(Level.INFO, "Received DATA_REQ from {0}",
								new Object[] { from.toString() });
                                                break;
					case MessageType.ACK:
						receiveAck(fromID, rpcID);
						break;
					default:
						LOGGER.log(Level.INFO,
								"Received unknown command from {0}: [{1}]{2}",
								new Object[] { from.toString(), messageType,
										new String(buffer.array()) });
					}

					if (updateRT) {
						node.updateBuckets(new NodeIdentifier(Node.ID_BITS,
								fromID.getBytes(), from));
					}

				} else {
					// If nothing has been read/received wait and read/receive
					// again
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				buffer.clear();

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void receiveAck(NodeIdentifier fromID, Identifier rpcID) {
		// This should be the either answer to a prior STORE or FOUND_VALUE ->
		// mark this RPC ID as received
		node.receivedRPC(fromID, rpcID);
	}

	private void receiveFoundValue(NodeIdentifier fromID, Identifier rpcID) {
		Identifier idToFind = getIDFromBuffer();
                node.lastlookup = fromID;
		// TODO Auto-generated method stub
		// Node kontaktieren, damit Datei gesendet werden kann.

		// This should be the answer to a prior FIND_VALUE -> mark this RPC ID
		// as received
		node.receivedRPC(fromID, rpcID);

		LOGGER.log(Level.INFO, "Received [FOUND VALUE on Node {0}] from Node {1}",
				new Object[] { idToFind, fromID });
	}

	private void receiveValueNodes(NodeIdentifier fromID, Identifier rpcID) {
		int numReceived = 0;

		// This is just for the log message
		StringBuilder nodes = new StringBuilder();

		while (buffer.hasRemaining()) {
			NodeIdentifier newID = getNodeTripleFromBuffer();
			node.sendFindValue(newID, node.getSearchID());
			nodes.append(newID).append(", ");
			numReceived++;
		}

		// This should be the answer to a prior FIND_VALUE -> mark this RPC ID
		// as
		// received
		node.receivedRPC(fromID, rpcID);

		LOGGER.log(Level.INFO,
				"Received {0} [VALUE NODES] [{1}] from Node {2})",
				new Object[] { numReceived, nodes.toString(), fromID });
	}

	private void receiveData(NodeIdentifier fromID, Identifier rpcID) {
		
		String data = new String(buffer.array());		
		String parts[] = data.split("-");
		
		String fileID = parts[0];
                int chunkCount = Integer.parseInt(parts[1]);
		int chunkID = Integer.parseInt(parts[2]);
		String chunkContent = parts[3];
                LOGGER.log(Level.INFO,"recieved Chunk file: "+fileID+" count: "+chunkCount+" id: "+chunkID);

                FileIdentifier fid = new FileIdentifier(1,fileID.getBytes());
                if(chunklist.get(fid) == null){
                    chunklist.put(fid, new HashMap<Integer,String>());
                }
                
                if(chunklist.get(fid).get(chunkID) == null){
                	chunklist.get(fid).put(chunkID, chunkContent);
                }

                if(chunklist.get(fid).size() >= chunkCount){
                    LOGGER.log(Level.INFO,"FILE complete file: "+fileID+" count: "+chunkCount+" id: "+chunkID);
                    String file = "";
                    for(int i=0; i<chunklist.get(fid).size();i++){                        
                        file += chunklist.get(fid).get(i);
                    }
                    node.store(fid);
                    node.storeData(fid, file);
                    chunklist.remove(fid);
                    LOGGER.log(Level.INFO,"FILE DATA: "+file);
                }          
		
                node.sendAck(fromID, rpcID);

		LOGGER.log(Level.INFO, "Received [DATA] [{0}] from Node {1})",
				new Object[] { data.toString(), fromID });

	}

        private void receiveDataReq(NodeIdentifier fromID, Identifier rpcID) {
            Identifier fid = getIDFromBuffer();
            //FileIdentifier fid = new FileIdentifier(1, buffer.array());
            node.sendData(fromID, fid);                        
            node.sendAck(fromID, rpcID);
        }

	private void receivePong(NodeIdentifier fromID, Identifier rpcID) {
		LOGGER.log(Level.INFO, "Received [PONG] from {0}",
				new Object[] { fromID });

		// This should be the answer to a prior PING -> mark this RPC ID as
		// received
		node.receivedRPC(fromID, rpcID);
	}

	private void receivePing(NodeIdentifier fromID, Identifier rpcID) {
		LOGGER.log(Level.INFO, "Received [PING] from {0}",
				new Object[] { fromID });
		node.sendPong(fromID, rpcID);
	}

	private void receiveNodes(NodeIdentifier fromID, Identifier rpcID) {

		int numReceived = 0;

		// This is just for the log message
		StringBuilder nodes = new StringBuilder();

		while (buffer.hasRemaining()) {
			NodeIdentifier newID = getNodeTripleFromBuffer();
			node.updateBuckets(newID);
			nodes.append(newID).append(", ");
			numReceived++;
		}

		// This should be the answer to a prior FIND_NODE -> mark this RPC ID as
		// received
		node.receivedRPC(fromID, rpcID);

		LOGGER.log(Level.INFO, "Received {0} [NODES] [{1}] from Node {2})",
				new Object[] { numReceived, nodes.toString(), fromID });
	}

	private void receiveFindNode(NodeIdentifier fromID, Identifier rpc_id) {
		Identifier idToFind = getIDFromBuffer();

		LOGGER.log(Level.INFO, "Received [FIND_NODE {0}] from Node {1}",
				new Object[] { idToFind, fromID });

		node.sendClosestNodesTo(fromID, idToFind, rpc_id, true);
	}

	private void receiveStore(NodeIdentifier fromID, Identifier rpcID) {
		Identifier fileID = getIDFromBuffer();

		LOGGER.log(Level.INFO, "Received [STORE {0}] from Node {1}",
				new Object[] { fileID, fromID });

		node.storePair(fileID, fromID);

		node.sendAck(fromID, rpcID);
	}

	private void receiveFindValue(NodeIdentifier fromID, Identifier rpcID) {
		Identifier fileID = getIDFromBuffer();

		LOGGER.log(Level.INFO, "Received [FIND VALUE {0}] from Node {1}",
				new Object[] { fileID, fromID });

		if (node.hasKey(fileID)) {
			node.sendFoundValue(fromID, fileID, rpcID);
		} else {
			node.sendClosestNodesTo(fromID, fileID, rpcID, false);
		}
	}

	public void terminate() {
		running = false;
	}
}
