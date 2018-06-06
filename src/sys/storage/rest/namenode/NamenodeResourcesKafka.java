package sys.storage.rest.namenode;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

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
	volatile AtomicBoolean waiting;
	boolean error;
	private List<String> output;
	volatile boolean  isProcessing;
	public NamenodeResourcesKafka() {
		this.kafka = new KafkaClient(TOPIC);
		gson = new Gson();
		namenode = new NamenodeResources();
		waiting = new AtomicBoolean(false);
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
			isProcessing=true;
			for(ConsumerRecord<String,String> record:records) {

				try {
					//System.out.println("RECEIVED RECORD: ID=" + record.key());
					String json = record.value();
					KafkaNamenodeObject o = gson.fromJson(json,KafkaNamenodeObject.class );
					String type = o.type;
					if(type.equals("create"))
						namenode.create(o.name, o.metadata);
					else if(type.equals("delete"))
						namenode.delete(o.name);
					else if(type.equals("update"))
						namenode.update(o.name, o.metadata);

					else {
						//System.out.println("recebi outra:" + type);
					}

				} 
				catch(WebApplicationException w) {
					error=true;
				}
				catch (Exception e) {
					//System.out.println("Erro na thread");
				}
				if(record.key().equals(getId())) 
					done();

				else {
					//System.out.println("KEYS DIFERENTES!; record.key:" + record.key() + "; getId()" + getId());
					//waiting.set(true);
				}

			}
			isProcessing = false;

		}	
	}

	private String getId() {
		return lastid;
	}

	private void setId(String id) {
		lastid = id;
		waiting.set(true);
	}

	private void done() {
		waiting.set(false);
	}


	@Override
	synchronized public List<String> list(String prefix) {

		while(isProcessing) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return namenode.list(prefix);
	}

	@Override
	synchronized  public void create(String name,  List<String> metadata) {
		String key = Random.key128();
		setId(key);
		kafka.write(TOPIC, new KafkaNamenodeObject(name, metadata, "create"), key);

		//System.out.println("Sending OP: create "+ key);
		while(waiting.get()) {
			//System.out.println("Enter Wait create"  + waiting.get() +" KEY:" + key + " ID:" + getId());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("Exit Wait create");
		if(error)
			throw new WebApplicationException( Status.CONFLICT );
	}

	@Override
	synchronized public void delete(String prefix) {
		String key = Random.key128();
		setId(key);
		kafka.write(TOPIC, new KafkaNamenodeObject(prefix,null, "delete"), key);

		//System.out.println("Sending OP: delete "+ key);
		while(waiting.get()) {
			//System.out.println("Enter Wait delete"  + waiting.get() +" KEY:" + key + " ID:" + getId());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("Exit Wait delete");
		if(error)
			throw new WebApplicationException( Status.NOT_FOUND );

	}

	@Override
	synchronized public void update(String name, List<String> metadata) {
		String key = Random.key128();
		setId(key);
		kafka.write(TOPIC, new KafkaNamenodeObject(name, metadata, "update"), key);

		//System.out.println("Sending OP: update "+ key);
		while(waiting.get()) {
			//System.out.println("Enter Wait update"  + waiting.get() +" KEY:" + key + " ID:" + getId());
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("Exit Wait update");
		if(error)
			throw new WebApplicationException( Status.NOT_FOUND );
	}

	@Override
	synchronized public List<String> read(String name) {
		while(isProcessing) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println("Exit Wait list");

		return namenode.read(name);
	}
	
}
