package utils.kafka;

import java.io.Serializable;
import java.util.List;

public class KafkaNamenodeObject  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String name; 
	public List<String> metadata; 
	public String type;
	
	
	public KafkaNamenodeObject(String name, List<String> metadata, String type) {
		this.name = name;
		this.metadata = metadata;
		this.type = type;
	}
	
	
}
