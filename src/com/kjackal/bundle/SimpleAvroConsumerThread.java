package com.kjackal.bundle;
 
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;


import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class SimpleAvroConsumerThread implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
 
    public SimpleAvroConsumerThread(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()){
    		Schema.Parser parser = new Schema.Parser();
    		Schema schema = parser.parse(SimpleAvroProducer.USER_SCHEMA);
    		Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(schema);

            GenericRecord record = recordInjection.invert(it.next().message()).get();

            System.out.println("Entity = " + record.get("Entity")
                    + ", Amount = " + record.get("Amount"));

        }
        
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}