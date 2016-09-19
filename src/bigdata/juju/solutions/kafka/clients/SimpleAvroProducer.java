package bigdata.juju.solutions.kafka.clients;

// From http://aseigneurin.github.io/2016/03/04/kafka-spark-avro-producing-and-consuming-avro-messages.html

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.Properties;

public class SimpleAvroProducer {

	public static final String USER_SCHEMA = "{" 
	        + "\"type\":\"record\"," 
			+ "\"name\":\"transaction\"," 
	        + "\"fields\":["
			+ "  { \"name\":\"Entity\", \"type\":\"string\" }," 
			+ "  { \"name\":\"Country\", \"type\":\"string\" }," 
			+ "  { \"name\":\"Address\", \"type\":\"string\" }," 
	        + "  { \"name\":\"PostCode\", \"type\":\"string\" },"
			+ "  { \"name\":\"Latitude\", \"type\":\"double\" }," 
			+ "  { \"name\":\"Longitude\", \"type\":\"double\" }," 
	        + "  { \"name\":\"Amount\", \"type\":\"double\" },"
	        + "  { \"name\":\"Currency\", \"type\":\"string\" }"
	        + "]}";

	public static void main(String[] args) throws InterruptedException {

    	for (String arg : args) {
			if (arg.equals("--help")){
				help();
				System.exit(0);
			}
		}

        long events = Long.parseLong(args[0]);
        String brokers = args[1];
        String topic = args[2];

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(USER_SCHEMA);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

		Properties props = new Properties();
		props.put("metadata.broker.list", brokers);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("request.required.acks", "0");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

		for (long event = 0; event < events; event++) {
			GenericData.Record avroRecord = new GenericData.Record(schema);
			avroRecord.put("Entity", "Shop " + event); 
			avroRecord.put("Country", "GR"); 
			avroRecord.put("Address", "Right Here"); 
			avroRecord.put("PostCode", "123456");
			avroRecord.put("Latitude", 1.2); 
			avroRecord.put("Longitude", 0.3); 
			avroRecord.put("Amount", 6.023);
        	avroRecord.put("Currency", "$");

			byte[] bytes = recordInjection.apply(avroRecord);

			KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, bytes);
			producer.send(data);

		}
		producer.close();

	}
	
	private static void help() {
		System.out.println("Put string messages to an Apache Kafka queue. Arguments:");
		System.out.println("Arg 1: number of messages to send");
		System.out.println("Arg 2: broker list, eg 183.112.213.250:9092");
		System.out.println("Arg 3: topic");
	}
}