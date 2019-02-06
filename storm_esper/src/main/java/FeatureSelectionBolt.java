import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class FeatureSelectionBolt implements IBasicBolt {
    private static final long serialVersionUID = 2L;
    private EsperFSOperation esperFSOperation;
    public FeatureSelectionBolt() {
    }
    public void execute(Tuple input, BasicOutputCollector collector) {


        Map <String, Object> fsEvent = (Map)input.getValueByField("feature_selection");
        esperFSOperation = new EsperFSOperation();
        esperFSOperation.esperPutFS(fsEvent);

    }
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
    }
    public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub

        return null;
    }
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            // create the instance of ESOperations class
            esperFSOperation = new EsperFSOperation();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
    public void cleanup() {
    }
}