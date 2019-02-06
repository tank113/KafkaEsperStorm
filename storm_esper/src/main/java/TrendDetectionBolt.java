import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class TrendDetectionBolt implements IBasicBolt {
    private static final long serialVersionUID = 2L;
    private EsperTDOperation esperTDOperation;
    public TrendDetectionBolt() {
    }
    public void execute(Tuple input, BasicOutputCollector collector) {
        Map <String, Object> fsEvent = (Map)input.getValueByField("detect_trend");
        esperTDOperation = new EsperTDOperation();
        esperTDOperation.esperPutTD(fsEvent);

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
            esperTDOperation = new EsperTDOperation();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
    public void cleanup() {
    }
}