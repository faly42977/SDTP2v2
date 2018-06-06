package sys.storage.io;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import utils.Hash;

public class BackupManager {

	Map<String, byte[]> backups;
	Map<String, byte[]> hashes;
	public BackupManager() {
		this.backups = new HashMap<String, byte[]>();
		this.hashes = new HashMap<String,byte[]>();
	}
	
	public void generateBackup(String id,  byte[] backup) {
		this.backups.put(id, backup);
		this.hashes.put(id, genHash(backup));
	}
	
	public boolean hasBackup(String id) {
		return this.backups.containsKey(id);
	}
	
	public boolean verifyHash(String id, byte[] hash) {
		//System.out.println("comparing hashes:");
		//System.out.println(hashes.get(id)+"<->"+hash);
		if (hashes.containsKey(id) && Arrays.equals(hash, hashes.get(id)))
			return true;
		else 
			return false;
	}
	
	public byte[] getHash(String id) {
		//System.out.println("getting hash for: " + id);
		//System.out.println("A Hash é_" +hashes.get(id) );
		return hashes.get(id);
	}
	
	public  byte[] getBackup (String id) {
		return backups.get(id);
	}

	public byte[] genHash(byte[] data) {
		return Hash.md5(data);
	}
	
}
