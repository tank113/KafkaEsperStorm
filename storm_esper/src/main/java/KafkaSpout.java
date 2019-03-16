import com.espertech.esper.client.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.io.IOException;
import java.util.*;

public class KafkaSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector spoutOutputCollector;
    private static Map<String, Object> result = new HashMap<String, Object>();

    //private static final Map<Integer, String> PRODUCT = new HashMap<Integer, String>();

    /**
     * Initialize Kafka configuration
     */
    public KafkaSpout(SpoutConfig kafkaConfig) throws InterruptedException {



        ConsumerThread consumerRunnable = new ConsumerThread(kafkaConfig.zkServers, kafkaConfig.topic, kafkaConfig.zkRoot, kafkaConfig.id);
        consumerRunnable.start();

        //consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {

        private String topicName;
        private String groupId;
        private String zRoot;
        public List<String> zkHost;
        private KafkaConsumer<String, JsonNode> kafkaConsumer;



        public ConsumerThread(List<String> zkHosts, String topicName, String zRoot ,String groupId) {
            this.zkHost = zkHosts;
            this.topicName = topicName;
            this.groupId = groupId;
            this.zRoot = zRoot;
        }

        public void run() {
            ConsumerProperties prop = new ConsumerProperties();
            Properties props = prop.propertiesKafkaConsumer(this.groupId, this.zkHost);
            kafkaConsumer = new KafkaConsumer<String, JsonNode>(props);
            // Subscribe to the topic.
            kafkaConsumer.subscribe(Collections.singletonList(topicName));
            System.out.println("consumer" + kafkaConsumer);
            //ObjectMapper mapper = new ObjectMapper();
            final int giveUp = 100;
            int noRecordsCount = 0;


            //Start processing messages

            KafkaEventProducer kProd = new KafkaEventProducer();

            try {
                while (true) {
                    ConsumerRecords<String, JsonNode> consumerRecords = kafkaConsumer.poll(100);
                    if (consumerRecords.count() == 0) {
                        noRecordsCount++;
                        if (noRecordsCount > giveUp) break;
                        else continue;
                    }
                    if (consumerRecords.isEmpty()) {
                        System.out.println("empty");
                    } else {

                        for (ConsumerRecord<String, JsonNode> record : consumerRecords) {
                            JsonNode jsonNode = record.value();
                            ObjectMapper mapper = new ObjectMapper();

                            result = mapper.readValue(jsonNode.toString(),
                                    new TypeReference<HashMap<String, Object>>() {
                                    });


                        }


                        kafkaConsumer.commitAsync();

                    }


                }
            }catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



        public KafkaConsumer<String, JsonNode> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }


    public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
    // Open the spout
        this.spoutOutputCollector = spoutOutputCollector;
    }
    public void nextTuple() {
    // Storm cluster repeatedly call this method to emit the continuous //
    // stream of tuples.


        spoutOutputCollector.emit(new Values(result.toString()));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
    // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {

        declarer.declare(new Fields(result.toString()));
    }
}

