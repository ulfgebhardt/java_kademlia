package node;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import util.BufferUtil;

/**
 * Same as a {@link Identifier}, but this also stores an IP address.
 * 
 * @author jln
 * 
 */
public class NodeIdentifier extends Identifier {

    private InetSocketAddress address;

    public NodeIdentifier(int size, byte[] bytes, InetSocketAddress address) {
        super(size, bytes);
        this.address = address;
    }

    public byte[] getTripleAsBytes() {
        ByteBuffer result = ByteBuffer.allocate(Node.SIZE_IP_ADDRESS
                + (Node.ID_BITS / 8));

        result.put(BufferUtil.addrToBytes(address));
        result.put(bits.toByteArray());
        return result.array();
    }

    public InetSocketAddress getAddress() {
        return address;
    }
}