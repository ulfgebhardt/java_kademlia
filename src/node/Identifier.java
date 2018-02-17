package node;

import java.math.BigInteger;
import java.util.BitSet;
import java.util.Random;

/**
 * A Kademlia identifier. Can be used for identifying files as well as nodes
 * (but for nodes check {@see NodeIdentifier}).
 * 
 * @author jln
 * 
 */
public class Identifier {
    private static Random random = new Random(System.currentTimeMillis());

    protected BitSet bits;

    private int size;

    public Identifier(int size, byte[] bytes) {
        this.size = size;        
        this.bits = BitSet.valueOf(bytes);
    }

    private Identifier(int size, BitSet bits) {
        this.size = size;
        this.bits = bits;
    }

    /**
     * Creates an ID exactly "in the middle" of the ID space. (If the ID space
     * is 8 bit wide, this returns an ID valued 128).
     * 
     * @param size
     *            the size of the id space
     * @return an Identifier
     */
    public static Identifier getStaticIdentifier(int size) {
        BitSet middle = new BitSet(size);
        middle.set(size - 1);
        return new Identifier(size, middle);
    }

    /**
     * Creates a random ID for the given id space size.
     * 
     * @param size
     *            the size of the id space
     * @return a random Identifier
     */
    public static Identifier getRandomIdentifier(int size) {
        BitSet bits = new BitSet(size);

        for (int i = 0; i < size; i++) {
            double threshold = random.nextGaussian();
            if (threshold > 0) {
                bits.set(i);
            }
        }

        return new Identifier(size, bits);
    }

    public BigInteger distanceTo(Identifier otherID) {
        BitSet distance = (BitSet) bits.clone();
        distance.xor(otherID.bits);
        return new BigInteger(1, distance.toByteArray());
    }

    /**
     * Returns whether the bit at the given position is set or not. The MSB is
     * at position 0.
     * 
     * @param index
     *            the index to check
     * @return true if the bit is set
     */
    public boolean isBitSetAt(int index) {
        BigInteger intValue = new BigInteger(1, bits.toByteArray());
        int numOfTrimmedZeros = size - intValue.bitLength();

        if (index < numOfTrimmedZeros) {
            return false;
        }

        return bits.get(bits.length() - (index + numOfTrimmedZeros) - 1);
    }

    public byte[] getBytes() {
        return bits.toByteArray();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Identifier)) {
            return false;
        } else {
            return bits.equals(((Identifier) o).bits);
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public String toString() {
        return new BigInteger(1, bits.toByteArray()).toString();
    }
}
