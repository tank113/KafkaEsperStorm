import java.util.List;
import java.util.Properties;

public class ConsumerProperties {

    public Properties propertiesKafkaConsumer(String groupId, List<String> hosts){

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", hosts);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.timeout.ms", "500");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");

        return props;
    }
}
