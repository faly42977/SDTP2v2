package sys.storage.io;

import java.util.HashMap;
import java.util.Map;

public class BackupManager {

	Map<String, byte[]> backups;
	Map<String, Integer> hashes;
	public BackupManager() {
		this.backups = new HashMap<String, byte[]>();
		this.hashes = new HashMap<String,Integer>();
	}
	
	public void generateBackup(String id,  byte[] backup, Integer hash) {
		this.backups.put(id, backup);
		this.hashes.put(id, hash);
	}
	
	public boolean hasBackup(String id) {
		return this.backups.containsKey(id);
	}
	
	public boolean verifyHash(String id, Integer hash) {
		System.out.println("comparing hashes:");
		System.out.println(hashes.get(id)+"<->"+hash);
		if (hashes.containsKey(id) && hashes.get(id) == hash)
			return true;
		else 
			return false;
	}
	
	public Integer getHash(String id) {
		System.out.println("getting hash for: " + id);
		System.out.println("A Hash é_" +hashes.get(id) );
		return hashes.get(id);
	}
	
	public  byte[] getBackup (String id) {
		return backups.get(id);
	}
	
}
