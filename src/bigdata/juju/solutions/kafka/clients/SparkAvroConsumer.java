package bigdata.juju.solutions.kafka.clients;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.cluster.Broker;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkAvroConsumer {

	public static String getConnectionString(String zkconnsction) throws IOException, KeeperException, InterruptedException{
        ZooKeeper zk = new ZooKeeper(zkconnsction, 10000, null);
        List<String> brokerList = new ArrayList<String>();

        List<String> ids = zk.getChildren("/brokers/ids", false);
        for (String id : ids) {
            String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
            Broker broker = Broker.createBroker(Integer.valueOf(id), brokerInfo);
            if (broker != null) {
                brokerList.add(broker.getConnectionString());
            }
            System.out.println(id + ": " + brokerInfo);
        }

        return String.join(",", brokerList);
	}

	
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
    	    	
        String brokers = getConnectionString(args[0]);
        String topic = args[1];
        
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox").set("spark.local.dir", "/tmp/spark-temp");;
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        Set<String> topics = Collections.singleton(topic);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(new Function<JavaPairRDD<String, byte[]>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, byte[]> rdd) throws Exception {
			    rdd.foreach(new VoidFunction<Tuple2<String, byte[]>>() {
					@Override
					public void call(Tuple2<String, byte[]> avroRecord) throws Exception {						
					    Schema.Parser parser = new Schema.Parser();
					    Schema schema = parser.parse(SimpleAvroProducer.USER_SCHEMA);
					    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
					    GenericRecord record = recordInjection.invert(avroRecord._2).get();
						System.out.println("Entity = " + record.get("Entity")
						    			+ ", Amount = " + record.get("Amount") + "\n");
					}
				});
				return null;
			}
		});	
        ssc.start();
        ssc.awaitTermination();
    }
}
