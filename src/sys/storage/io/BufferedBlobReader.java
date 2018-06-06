package sys.storage.io;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import api.storage.BlobStorage.BlobReader;
import api.storage.Datanode;
import api.storage.Namenode;

/*
 * 
 * Implements BlobReader.
 * 
 * Allows reading or iterating the lines of Blob one at a time, by fetching each block on demand.
 * 
 * Intended to allow streaming the lines of a large blob (with many (large) blocks) without reading it all first into memory.
 */
public class BufferedBlobReader implements BlobReader {

	final String name;
	final Namenode namenode; 
	final Datanode datanode;

	final Iterator<String> blocks;
	public BackupManager backups;
	final LazyBlockReader lazyBlockIterator;

	public BufferedBlobReader( String name, Namenode namenode, Datanode datanode, BackupManager backups ) {
		this.name = name;
		this.namenode = namenode;
		this.datanode = datanode;
		this.backups  = backups;
		this.blocks = this.namenode.read( name ).iterator();
		this.lazyBlockIterator = new LazyBlockReader();
		this.backups = backups;
	}

	@Override
	public String readLine() {
		return lazyBlockIterator.hasNext() ? lazyBlockIterator.next() : null ;
	}

	@Override
	public Iterator<String> iterator() {
		return lazyBlockIterator;
	}

	private Iterator<String> nextBlockLines() {
		if( blocks.hasNext() )
			return fetchBlockLines( blocks.next() ).iterator();
		else 
			return Collections.emptyIterator();
	} 

	private List<String> fetchBlockLines(String block) {
		try {
			byte[] data = datanode.readBlock( block );
			byte[] hash = backups.genHash(data);

			//System.out.println("Working for hash: " + hash);

			if (Arrays.equals(backups.getHash(block),hash)) 
				return Arrays.asList( new String( backups.getBackup(block) ).split("\\R"));

			else 
				return Arrays.asList(new String("CORRUPTED BLOCK"));


		} catch (Exception e) {
			//System.out.println("Error on fetchBlockLines");
			e.printStackTrace();

		}
		return null;
	}

	private class LazyBlockReader implements Iterator<String> {

		Iterator<String> lines;

		LazyBlockReader() {
			this.lines = nextBlockLines();
		}

		@Override
		public String next() {
			return lines.next();
		}

		@Override
		public boolean hasNext() {
			return lines.hasNext() || (lines = nextBlockLines()).hasNext();
		}	
	}
}

