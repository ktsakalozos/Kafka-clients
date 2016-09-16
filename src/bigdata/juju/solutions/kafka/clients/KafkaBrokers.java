package bigdata.juju.solutions.kafka.clients;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.ZooKeeper;

import kafka.cluster.Broker;

public class KafkaBrokers {

	public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(args[0], 10000, null);
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

        System.out.println("Connection string : " + String.join(",", brokerList));
    }
}
