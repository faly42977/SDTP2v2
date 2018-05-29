package sys.storage.rest.namenode;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response.Status;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.google.gson.Gson;

import api.storage.Namenode;
import utils.Random;
import utils.kafka.KafkaClient;
import utils.kafka.KafkaNamenodeObject;

public class NamenodeResourcesKafka implements Namenode {
	private static String TOPIC = "SDT";
	private static int SLEEP_TIME = 10;

	private KafkaClient kafka;

	private Gson gson;
	private NamenodeResources namenode;
	volatile private String lastid;
	volatile boolean waiting;
	boolean error;
	private List<String> output;

	public NamenodeResourcesKafka() {
		this.kafka = new KafkaClient(TOPIC);
		gson = new Gson();
		namenode = new NamenodeResources();
		waiting = false;
		error=false;
		Thread kafkaThread = new Thread(()->{
			KafkaProcessor();
		});

		kafkaThread.start();
	}

	public void KafkaProcessor() {
		while (true) {
			ConsumerRecords<String, String> records = kafka.read();
			error = false;
			for(ConsumerRecord<String,String> record:records) {
				try {
					System.out.println("RECEIVED RECORD: ID=" + record.key());
					String json = record.value();
					KafkaNamenodeObject o = gson.fromJson(json,KafkaNamenodeObject.class );
					String type = o.type;
					if(type.equals("create"))
						namenode.create(o.name, o.metadata);
					else if(type.equals("delete"))
						namenode.delete(o.name);
					else if(type.equals("update"))
						namenode.update(o.name, o.metadata);
					else if(type.equals("read"))
						output=namenode.read(o.name);
					else if(type.equals("list"))
						output=namenode.list(o.name);
				} 
				catch(WebApplicationException w) {
					error=true;
				}
				catch (Exception e) {
					System.out.println("Erro na thread");
				}
				if(record.key().equals(getId())) 
					done();
			}
		}	
	}

	synchronized private String getId() {
		return lastid;
	}

	synchronized private void setId(String id) {
		lastid = id;
		waiting = true;
	}

	synchronized private void done() {
		waiting = false;
	}


	@Override
	synchronized public List<String> list(String prefix) {
		String key = Random.key128();
		kafka.write(TOPIC, new KafkaNamenodeObject(prefix, null, "list"), key);
		setId(key);
		while(waiting) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(error)
			throw new WebApplicationException( Status.NOT_FOUND );
		return output;
	}

	@Override
	synchronized public void create(String name,  List<String> metadata) {
		String key = Random.key128();
		kafka.write(TOPIC, new KafkaNamenodeObject(name, metadata, "create"), key);
		setId(key);
		while(waiting) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(error)
			throw new WebApplicationException( Status.CONFLICT );
	}

	@Override
	synchronized public void delete(String prefix) {
		String key = Random.key128();
		kafka.write(TOPIC, new KafkaNamenodeObject(prefix,null, "delete"), key);
		setId(key);
		while(waiting) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(error)
			throw new WebApplicationException( Status.NOT_FOUND );

	}

	@Override
	synchronized public void update(String name, List<String> metadata) {
		String key = Random.key128();
		kafka.write(TOPIC, new KafkaNamenodeObject(name, metadata, "update"), key);
		setId(key);
		while(waiting) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(error)
			throw new WebApplicationException( Status.NOT_FOUND );
	}

	@Override
	synchronized public List<String> read(String name) {
		String key = Random.key128();
		kafka.write(TOPIC, new KafkaNamenodeObject(name, null, "read"), key);
		setId(key);
		System.out.println("Setting ID:" + key);
		while(waiting) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(error)
			throw new WebApplicationException( Status.NOT_FOUND );
		return output;
	}
}
