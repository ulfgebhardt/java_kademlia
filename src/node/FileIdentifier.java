package node;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class FileIdentifier extends Identifier {
	
	private String fileID;

	public FileIdentifier(int size, byte[] fileID) {
		super(size, fileID);
		
		/*//calculate SHA-256 Hash of key
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(fileID.getBytes());
			this.fileID = md.digest().toString();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
	}
	
	public String getKey() {
		return this.fileID;
	}
}
