package message;

public class MessageType {
    public final static byte FIND_NODE  	= 0;
    public final static byte NODES      	= 1;

    public final static byte PING       	= 11;
    public final static byte PONG       	= 12;

    public final static byte LEAVE      	= 2;

    public final static byte FIND_VALUE 	= 4;
    public final static byte STORE      	= 5;
    public final static byte DATA       	= 6;
    public final static byte DATA_REQ           = 7;
    public final static byte VALUE_NODES	= 8;
    public final static byte FOUND_VALUE 	= 9;
    public final static byte ACK                = 10;
}
