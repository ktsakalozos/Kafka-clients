package bigdata.juju.solutions.kafka.clients;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class SparkAvroConsumer {


    public static void main(String[] args) {
        String brokers = args[0];
        String topic = args[1];
    	
        SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox");
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
			            	+ ", Amount = " + record.get("Amount"));
					}
				});
				return null;
			}
		});

        ssc.start();
        ssc.awaitTermination();
    }
}
