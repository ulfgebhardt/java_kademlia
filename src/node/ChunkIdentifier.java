package node;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ChunkIdentifier extends Identifier {
	
	private String chunkID;
	private FileIdentifier fileID;

	public ChunkIdentifier(int size, byte[] bytes, FileIdentifier fileID, String chunkID) {
		super(size, bytes);
		
		this.fileID = fileID;
		
		//calculate SHA-256 Hash of chunckID
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(chunkID.getBytes());
			this.chunkID = md.digest().toString();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public String getChunkID() {
		return this.chunkID;
	}
	
	public FileIdentifier getFileID(){
		return this.fileID;
	}

}
