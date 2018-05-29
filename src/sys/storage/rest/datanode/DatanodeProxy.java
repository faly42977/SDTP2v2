package sys.storage.rest.datanode;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import api.storage.Datanode;
import utils.dropbox.DropboxClient;

public class DatanodeProxy implements Datanode {
	private String address;


	Map<String, byte[]> cache;

	public DatanodeProxy(String myURL) {
		this.address = myURL;
		this.cache = new HashMap<String, byte[]>();
	}
	public synchronized String createBlock(byte[] data, String blob) {
		String blockId= utils.Random.key64();
		cache.put(blockId, data);
		DropboxClient.createFile("/Datanode/"+blockId, data);
		return address + "datanode" + "/" + blockId;
	}

	@Override
	public synchronized  void deleteBlock(String block) {
		cache.remove(block);
		if(!DropboxClient.delete("/Datanode/" +block))
			throw new WebApplicationException(Status.NOT_FOUND);

	}

	@Override
	public synchronized  byte[] readBlock(String block) {
		if (cache.containsKey(block)) 
			return cache.get(block);

		byte[] file= DropboxClient.getFile("/Datanode/" +block);

		if(file==null)
			throw new WebApplicationException(Status.NOT_FOUND);
		return file;
	}

}
