import com.espertech.esper.client.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrendDetectionBolt extends BaseRichBolt implements UpdateListener{
    private static final long serialVersionUID = 2L;
    private EsperTDOperation esperTDOperation;
    private OutputCollector collector;
    private List<Object> tuple = new ArrayList<Object>();
    private Map<String, Object> result_map = new HashMap<>();
    private EPRuntime runtime = null;
    private transient EPServiceProvider epService;
    private int count=0;
    private Object[] objArr = new Object[5];
    private Map<String, Object> result_output = new HashMap<>();
    PushToInfluxDbBolt influx;
    public TrendDetectionBolt() {
    }
    public void execute(Tuple input) {

        // Take input from Feature selection bolt
        Fields tdEventKeys = input.getFields();
        //System.out.println("Keys" + tdEventKeys);
        List<Object> tdEventVal = input.getValues();
        Map<String, Object> result_output = new HashMap<String, Object>();
        for(int i=0; i<tdEventKeys.size();i++){
            result_output.put(tdEventKeys.get(i), tdEventVal.get(i));
            createObjectArray(tdEventVal.get(i));
        }
        System.out.println("FSBolt to TDBolt - Maps" + result_output);

        /*ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.convertValue(tdEvent, JsonNode.class);
        try {
            result_output = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map.Entry<String,Object> entry = result_output.entrySet().iterator().next();

        Map<String, Object> mapTD = mapper.convertValue(entry.getValue(), Map.class);
        System.out.println("TDBolt to query - map" + mapTD);*/

        //esperTDOperation = new EsperTDOperation();

        //esperTDOperation.esperPutTD(result_output);
        //collector.ack(input);

    }

    /*public void sendTDEventToBolt(Object resultTDEvent){

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.convertValue(resultTDEvent, JsonNode.class);
        try {
            result_map = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(Double val:result_map.values()){
            System.out.println("Values to emit - Trends" + val);
            collector.emit("tdBolt", new Values(val));
        }
        collector.emit("tdBolt", new Values(result_map.values()));

        //tuple.add(resultTDEvent);


    }*/

    public void createObjectArray(Object fsEvent){

        if(count<3){
            objArr[count++]=fsEvent;
        }
        else if(count==3){
            runtime.sendEvent(objArr, "weatherTrendEvent");
            count=0;
        }



    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        /*for(String s:result_map.keySet()){
            System.out.println("Fields to emit - Trends" + s);
            declarer.declareStream("tdBolt", new Fields(s));
        }*/

        declarer.declareStream("tdBolt", new Fields("detect_trend"));

    }
    public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub

        return null;
    }
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            // create the instance of ESOperations class
            //esperTDOperation = new EsperTDOperation();
            Configuration config = new Configuration();
            epService = EPServiceProviderManager.getDefaultProvider(config);

            if (!epService.getEPAdministrator().getConfiguration().isEventTypeExists("weatherTrendEvent")) {

                // Define Single-row function in ESPER
                epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("detect_trend", TrendDetection.class.getName(), "detect_trend");


                // Create WeatherTrendEvent using Map properties
                String createEventExpTrends = "@EventRepresentation(objectarray) create schema weatherTrendEvent as (prop1 Map)";
                EPStatement statementTrend = epService.getEPAdministrator().createEPL(createEventExpTrends);
                ListenerEvent listenerForTrend = new ListenerEvent();
                statementTrend.addListener(listenerForTrend);

                // Create Batch context of 1 seconds
            /*String expression2 = "create context batch10seconds start @now end after 1 sec";
            EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
            ListenerEvent listener2 = new ListenerEvent();
            statement2.addListener(listener2);*/
            }

            runtime = epService.getEPRuntime();

            // Esper Query for Trends detection using single row function which returns the nested map with increasing, decreasing and Turn trends features.
            String expressionTrend = "select detect_trend(trendEvent) from weatherTrendEvent.win:length_batch(3) as trendEvent";
            EPStatement statement = epService.getEPAdministrator().createEPL(expressionTrend);
            statement.addListener(this);

            this.collector=collector;

            influx = new PushToInfluxDbBolt();
        } catch (Exception e) {
            //throw new RuntimeException();
            e.printStackTrace();
        }
    }
    public void cleanup() {
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        System.out.println("event \t" + newEvents[0].getUnderlying() + "\n");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.convertValue(newEvents[0].getUnderlying(), JsonNode.class);
        try {
            result_output = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        //collector.emit("tdBolt", new Values(result_output.values()));
        influx.sendTDDataToDB(result_output);
    }
}