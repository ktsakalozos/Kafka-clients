package bigdata.juju.solutions.kafka.clients;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaSparkCassandraPipe {

	static int key=0;
    public static void main(String args[]) throws Exception{

        if(args.length != 3)
        {
            System.out.println("parameters not given properly");
            System.exit(1);
        }

        String brokers = SparkAvroConsumer.getConnectionString(args[0]);
        String topic = args[1];
        String cassandrahost = args[2];
        
        SparkConf conf = new SparkConf()
                .setAppName("kafka-to-cassandra")
                .set("spark.local.dir", "/tmp/spark-temp")
                .set("spark.cassandra.connection.host", cassandrahost);
                
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
        Session session = connector.openSession();
        session.execute("DROP KEYSPACE IF EXISTS juju_keyspace");
        session.execute("CREATE KEYSPACE juju_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("CREATE TABLE juju_keyspace.transactions (entity TEXT PRIMARY KEY, amount DOUBLE)");

        Set<String> topics = Collections.singleton(topic);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, StringDecoder.class, DefaultDecoder.class, kafkaParams, topics);

        JavaDStream<Transaction> data = directKafkaStream.map(new Function< Tuple2<String, byte[]>, Transaction>() {
			@Override
			public Transaction call( Tuple2<String, byte[]> rdd) throws Exception {
			    Schema.Parser parser = new Schema.Parser();
			    Schema schema = parser.parse(SimpleAvroProducer.USER_SCHEMA);
			    Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);
			    GenericRecord record = recordInjection.invert(rdd._2).get();
			    return new Transaction(record.get("Entity").toString(), Double.parseDouble(record.get("Amount").toString()));
			}
		});	

        data.foreachRDD(new Function<JavaRDD<Transaction>,Void>() {

			@Override
			public Void call(JavaRDD<Transaction> v1) throws Exception {
				// TODO Auto-generated method stub
				CassandraJavaUtil.javaFunctions(v1).writerBuilder("juju_keyspace", "transactions", CassandraJavaUtil.mapToRow(Transaction.class)).saveToCassandra();
				return null;
			}
        });
        
        ssc.start();
        ssc.awaitTermination();
    }
}