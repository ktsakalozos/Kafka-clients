package com.kjackal.bundle;

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

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(USER_SCHEMA);
		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

		long events = 10;
		Properties props = new Properties();
		// props.put("metadata.broker.list", "localhost:9092");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("partitioner.class", "com.kjackal.bundle.BTPartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			GenericData.Record avroRecord = new GenericData.Record(schema);
			avroRecord.put("Entity", "Shop " + nEvents); 
			avroRecord.put("Country", "GR"); 
			avroRecord.put("Address", "Right Here"); 
			avroRecord.put("PostCode", "123456");
			avroRecord.put("Latitude", 1.2); 
			avroRecord.put("Longitude", 0.3); 
			avroRecord.put("Amount", 6.023);
        	avroRecord.put("Currency", "$");

			byte[] bytes = recordInjection.apply(avroRecord);

			KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>("avrotopic", bytes);
			producer.send(data);

		}
		producer.close();

	}
}