package com.kjackal.bundle;

// https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
//http://kafka.apache.org/081/documentation.html#quickstart
//
// bin/kafka-create-topic.sh --topic page_visits --replica 3 --zookeeper localhost:2181 --partition 5
// or 
// ./bin/kafka-topics.sh --create --topic avrotopic --replication-factor 1 --zookeeper localhost:2181 --partition 1
// Verify
// bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic page_visits --from-beginning

// http://stackoverflow.com/questions/23228222/running-into-leadernotavailableexception-when-using-kafka-0-8-1-with-zookeeper-3

import java.util.*;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class BTKafkaProducer {
	
    public static void main(String[] args) {
    	
//        long events = Long.parseLong(args[0]);
        long events = Long.parseLong("10");
        Random rnd = new Random();
 
        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092");
        props.put("metadata.broker.list", "10.0.3.209:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "com.kjackal.bundle.BTPartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2."+ rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits4", ip, msg);
               producer.send(data);
        }
        producer.close();
    }
}