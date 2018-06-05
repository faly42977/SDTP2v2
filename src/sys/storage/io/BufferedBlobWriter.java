package sys.storage.io;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;

import api.storage.BlobStorage.BlobWriter;
import api.storage.Datanode;
import api.storage.Namenode;
import utils.IO;

/*
 * 
 * Implements a BlobWriter that can support multiple block writing policies.
 * 
 * Accumulates lines in a list of blocks, avoids splitting a line across blocks.
 * When the BlobWriter is closed, the Blob (and its blocks) is published to the Namenode.
 * 
 */
public class BufferedBlobWriter implements BlobWriter {
	final String name;
	final int blockSize;
	final ByteArrayOutputStream buf;
	BackupManager backups;
	final Namenode namenode; 
	final Datanode[] datanodes;
	List<String> blocks = new LinkedList<>();
	final BiFunction<String, Integer, Integer> storagePolicy;


	public BufferedBlobWriter(String name, Namenode namenode, Datanode[] datanodes, int blockSize, BiFunction<String,Integer, Integer> blockStoragePolicy, BackupManager backups) {
		this.name = name;
		this.namenode = namenode;
		this.datanodes = datanodes;
		this.backups = backups;
		this.blockSize = blockSize;
		this.buf = new ByteArrayOutputStream( blockSize );
		this.storagePolicy = blockStoragePolicy;
	}

	//selects the datanode based on the storage policy (uses the name and index of the block).
	private Datanode selectDatanode() {
		return datanodes[ storagePolicy.apply( name, blocks.size() ) % datanodes.length ];
	}

	private void flush( boolean eob ) {
		if( buf.size() > 0 ) {
			byte[] rawBlock =  buf.toByteArray();
			String block = selectDatanode().createBlock(rawBlock, name);
			blocks.add( block );
			
			try {
				Integer hash = Arrays.hashCode(rawBlock);
				backups.generateBackup(name, rawBlock, hash);
				System.out.println("name: " + name );
				System.out.println("rawBlock: " + rawBlock );
				System.out.println("hashCode: " + hash );
			}catch (Exception e) {
				System.out.println("Error creating hash");
				e.printStackTrace();
			}
		}

		if( eob && blocks.size() > 0 ) {
			namenode.create(name, blocks);
			blocks.clear();
		}
		buf.reset();
	}

	@Override
	public void writeLine(String line) {
		if( buf.size() + line.length() > blockSize - 1 ) {
			this.flush(false);
		}
		IO.write( buf, line.getBytes() );
		IO.write( buf, '\n');
	}

	@Override
	public void close() {
		flush( true );
	}	
}