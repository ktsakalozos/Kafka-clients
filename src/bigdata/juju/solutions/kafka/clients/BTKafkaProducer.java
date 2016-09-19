// https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example

//http://kafka.apache.org/081/documentation.html#quickstart
//
// bin/kafka-create-topic.sh --topic page_visits --replica 3 --zookeeper localhost:2181 --partition 5
// or 
// ./bin/kafka-topics.sh --create --topic avrotopic --replication-factor 1 --zookeeper localhost:2181 --partition 1
// Verify
// bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic page_visits --from-beginning

// http://stackoverflow.com/questions/23228222/running-into-leadernotavailableexception-when-using-kafka-0-8-1-with-zookeeper-3

package bigdata.juju.solutions.kafka.clients;

import java.util.*;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
 
public class BTKafkaProducer {
	
    public static void main(String[] args) {
    	
    	for (String arg : args) {
			if (arg.equals("--help")){
				help();
				System.exit(0);
			}
		}

        long events = Long.parseLong(args[0]);
        String brokers = args[1];
        String topic = args[2];
        		
        Random rnd = new Random();
 
        Properties props = new Properties();
        props.put("metadata.broker.list", brokers);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "bigdata.juju.solutions.kafka.clients.BTPartitioner");
        props.put("request.required.acks", "0");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, String> producer = new Producer<String, String>(config);
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();
               String ip = "10.55.61."+ rnd.nextInt(255); 
               String msg = runtime + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
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