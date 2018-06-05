package utils.kafka;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import com.google.gson.Gson;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class KafkaClient {

	private static final int SESSION_TIMEOUT = 2000;
	private static final int CONNECTION_TIMEOUT = 2000;
	private static final String ZOOKEEPER_SERVER = "zoo1:2181,zoo2:2181,zoo3:2181";
	private static final int REPLICATION_FACTOR = 1;
	ZkUtils zkUtils ;
	Properties props;
	Producer<String, String> producer;
	KafkaConsumer<String, String> consumer;
	Gson json;

	public KafkaClient(String topic) {
		props = new Properties();
		
		//Localização dos servidores kafka (lista de máquinas + porto)
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092,kafka3:9092");

		// Classe para serializar as chaves dos eventos (string)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		// Classe para serializar os valores dos eventos (string)
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		//Configura o modo de subscrição (ver documentação em kafka.apache.org)
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "grp" +  new Random().nextLong());
		// Classe para serializar as chaves dos eventos (string)
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		// Classe para serializar os valores dos eventos (string)
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		
		this.json = new Gson();
		
		ZkClient zkClient = new ZkClient(
				ZOOKEEPER_SERVER,
				SESSION_TIMEOUT,
				CONNECTION_TIMEOUT,
				ZKStringSerializer$.MODULE$);
		Properties topicConfig = new Properties();
		
		zkUtils = new ZkUtils(zkClient, new ZkConnection(ZOOKEEPER_SERVER), false);

		
			try {
				AdminUtils.createTopic(zkUtils, topic, 1, REPLICATION_FACTOR, topicConfig, null);	
			} catch( TopicExistsException e ) {	
				System.err.println("Topic " + topic + " already exists...");
			}
		this.zkUtils = new ZkUtils(zkClient, new ZkConnection(ZOOKEEPER_SERVER), false);
		this.producer = new KafkaProducer<>(props);
		this.consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
	}



	public void write(String topic, KafkaNamenodeObject kafkaNamenodeObject, String key) {
		producer.send(new ProducerRecord<String, String>(topic, key, json.toJson(kafkaNamenodeObject)));
	}

	public ConsumerRecords<String, String> read() {
		ConsumerRecords<String, String> records = consumer.poll(SESSION_TIMEOUT);
		return records;
	}


}
