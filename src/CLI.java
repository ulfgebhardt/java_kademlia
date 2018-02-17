import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.MappedByteBuffer;
import java.util.logging.LogManager;
import node.FileIdentifier;

import node.Identifier;
import node.Node;
import node.NodeIdentifier;

public class CLI {

    public static void main(String[] args) throws IOException {
        System.setProperty("java.util.logging.config.file",
                "logging.properties");

        try {
            LogManager.getLogManager().readConfiguration();
        } catch (Exception e) {
            e.printStackTrace();
        }

        Node node = new Node();

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String s;
        while ((s = in.readLine()) != null && s.length() != 0) {
            String[] splitted = s.split(" ");

            String cmd = splitted[0];            

            switch (cmd) {
            //status
            case "status":
                for (NodeIdentifier id : node.getNeighbors()) {
                    System.out.println(id);
                }
                break;
            //lookup fileID
            case "lookup":
                String fileID = splitted[1];
                // TODO not implemented
            	// Zum testen:
            	FileIdentifier fileIDToFind = new FileIdentifier(1, fileID.getBytes());
            	node.findValue(fileIDToFind);
                break;
            //request fileID
            case "request": 
                String fileID3 = splitted[1];
                FileIdentifier fileIDToFind2 = new FileIdentifier(1, fileID3.getBytes());
                node.sendDataReq(fileIDToFind2);
            	break;
            //leave
            case "leave":
                node.leave();
                break;
            //store fileID data
            case "store":
                String fileID2 = splitted[1];
                String data = splitted[2];
            	// TODO not implemented
            	// Zum testen:
            	FileIdentifier fileIDToStore = new FileIdentifier(1,fileID2.getBytes());
            	node.store(fileIDToStore);
                node.storeData(fileIDToStore,data);
            	break;
            default:
                System.out.println("Unknown command.");
                break;
            }
        }
    }
}