package sys.storage.rest.datanode;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import api.storage.Datanode;
import sys.storage.io.BackupManager;
import utils.Base58;
import utils.Hash;
import utils.IO;
import utils.Random;

public class DatanodeResources implements Datanode {

	private static final long MAX_BLOCKS_IN_CACHE = 128;

	protected final String baseURI;
	protected Map<String,String> hashes;
	protected BackupManager b = new BackupManager(); 

	protected Cache<String, byte[]> blockCache = Caffeine.newBuilder()
			.maximumSize( MAX_BLOCKS_IN_CACHE )
			.build();

	public DatanodeResources(final String baseURI) {
		this.baseURI = baseURI + Datanode.PATH.substring(1) + "/";
		hashes = new HashMap<String,String>();
	}

	// Second parameter is only used in the GC version...
	public String createBlock(byte[] data, String blob) {
		String id = Random.key128()+"-"+b.genHash(data);
		//System.out.println("-------->create: " +id);
		blockCache.put( id, data );
		IO.write( new File( id ), data);
		hashes.put(id, genHash(data));
		//System.out.println(hashes);
		return baseURI.concat(id);
	}

	public void deleteBlock(String block) {
		//System.out.println("-------->delete: " +block);
		blockCache.invalidate( block );		
		File file = new File(block);
		if (file.exists())
			IO.delete(file);
		else
			throw new WebApplicationException(Status.NOT_FOUND);
	}

	public byte[] readBlock(String block) {
		//System.out.println("-------->read: " +block);
		byte[] data = blockCache.getIfPresent( block );
		String hash = block.split("-")[1];
		if( data == null ) {
			File file = new File(block);
			if (file.exists()) {
				data = IO.read( file );
				blockCache.put( block, data);
				return markOrReturn(hash,data);			
			}			
		} else {
			return markOrReturn(hash,data);
		}
		throw new WebApplicationException(Status.NOT_FOUND);
	}

	private byte[] markOrReturn(String hash, byte[] data) {
		if(hash.equals(b.genHash(data)))
			return data;
		else
			return "CORRUPTED BLOCK".getBytes();
	}	

	private String genHash(byte[] data) {
		return Base58.encode(Hash.sha256(data));
	}

}



