// https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example

package bigdata.juju.solutions.kafka.clients;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
public class BTPartitioner implements Partitioner {
    public BTPartitioner (VerifiableProperties props) {
 
    }
 
    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        String stringKey = (String) key;
        int offset = stringKey.lastIndexOf('.');
        if (offset > 0) {
           partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
        }
       return partition;
  }
 
}