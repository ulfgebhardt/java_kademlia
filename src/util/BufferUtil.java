package util;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class BufferUtil {

	public static ByteBuffer clone(ByteBuffer original) {
		ByteBuffer clone = ByteBuffer.allocate(original.capacity());

		int oldPosition = original.position();
		original.rewind();// copy from the beginning
		clone.put(original);
		// original.rewind();
		original.position(oldPosition);
		clone.flip();
		return clone;
	}

    public static byte[] addrToBytes(InetSocketAddress addr) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        for (String part : addr.getHostString().split("\\.")) {
            buffer.put(Byte.valueOf(part));
        }
        buffer.putInt(addr.getPort());
        return buffer.array();
    }
}
