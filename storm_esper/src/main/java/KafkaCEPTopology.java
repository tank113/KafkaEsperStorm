import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaCEPTopology {
    public static void main(String[] args) {
        try {
            // ZooKeeper hosts for the Kafka cluster
            BrokerHosts zkHosts = new ZkHosts("localhost:2181");
            // Create the KafkaSpout configuartion
            // Second argument is the topic name
            // Third argument is the zookeepr root for Kafka
            // Fourth argument is consumer group id
            SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "sample", "", "id1");
            // Specify that the kafka messages are String
            // We want to consume all the first messages in the topic everytime
            // we run the topology to help in debugging. In production,this
            // property should be false
            kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
            kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
            KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
            // Now we create the topology
            TopologyBuilder builder = new TopologyBuilder();
            // set the kafka spout class
            builder.setSpout("KafkaSpout", kafkaSpout, 2);
             // set the word and sentence bolt class
            builder.setBolt("FeatureSelectionBolt", new FeatureSelectionBolt(), 1).globalGrouping("KafkaSpout");
            builder.setBolt("TrendDetectionBolt", new TrendDetectionBolt(), 1).globalGrouping("FeatureSelectionBolt");
            // create an instance of LocalCluster class for executing topology
            // in local mode.
            LocalCluster cluster = new LocalCluster();
            Config conf = new Config();
            conf.setDebug(true);
            if (args.length > 0) {
                conf.setNumWorkers(2);
                conf.setMaxSpoutPending(5000);
                StormSubmitter.submitTopology("KafkaCEPToplogy1", conf, builder.createTopology());
            } else {
                // Submit topology for execution
                cluster.submitTopology("KafkaCEPToplogy1", conf, builder.createTopology());
                System.out.println("called1");
                Thread.sleep(1000000);
                // Wait for sometime before exiting
                System.out.println("Waiting to consume from kafka");
                System.out.println("called2");
                // kill the KafkaCEPTopology
                cluster.killTopology("KafkaCEPToplogy1");
                System.out.println("called3");
                // shutdown the storm test cluster
                cluster.shutdown();
            }
        } catch (Exception exception) {
            System.out.println("Thread interrupted exception : " +
                    exception);
        }
    }
}
