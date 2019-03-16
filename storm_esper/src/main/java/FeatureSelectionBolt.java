import com.espertech.esper.client.*;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import scala.xml.Null;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FeatureSelectionBolt extends BaseRichBolt implements UpdateListener{
    private static final long serialVersionUID = 2L;
    private EsperFSOperation esperFSOperation;
    private OutputCollector collector;
    private List<Object> tuple = new ArrayList<Object>();
    private Map<String, Object> result_map = new HashMap<>();
    private Map<String, Object> result_output = new HashMap<>();
    private EPRuntime runtime = null;
    private Map<String, Object> fsEvent = new HashMap<String, Object>();
    private List<Map<String, Object>> list = new ArrayList<>();
    private transient EPServiceProvider epService;
    private int count=0;
    private Object[] objArr = new Object[5];
    PushToInfluxDbBolt influx;
    public FeatureSelectionBolt() {
    }


    public void execute(Tuple input) {

        // Take input from Kafka Spout
        List<Object> event = input.getValues();
        System.out.println("Spout to FSBolt" + event);


        Object value = event.get(4);

        String[] pairs = value.toString().split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            //System.out.println("keyvalue" + keyValue[0].trim());
            Object values = keyValue[1].trim().replace("}", "");
            //System.out.println("value" + value);
            fsEvent.put(keyValue[0].trim().replace("{", ""), values);
        }
        //System.out.println("FSBolt to ESPER query - map" + fsEvent);
        createObjectArray(value);


        //collector.ack(input);

        }


    /*public void sendFSEventToBolt(Object resultFSEvent){

        System.out.println("received from query - Object" + resultFSEvent);
        //tuple.add(resultFSEvent);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.convertValue(resultFSEvent, JsonNode.class);

        try {
            result_map = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }


        collector.emit(new Values(result_map.values()));
        collector.emit("fsBolt", new Values(result_map.values()));




        }*/


    public void createObjectArray(Object fsEvent){

        if(count<3){
            objArr[count++]=fsEvent;
        }
        else if(count==3){
            runtime.sendEvent(objArr, "weatherEvent");
            count=0;
        }



    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        /*for(String s:result_map.keySet()){
            System.out.println("Fields to emit" + s);
            declarer.declare(new Fields(s));
            declarer.declareStream("fsBolt", new Fields(s));
        }*/
        //System.out.println("check keys" + fsEvent);
        declarer.declare(new Fields("feature_selection"));
        declarer.declareStream("fsBolt", new Fields("feature_selection"));

    }
    public Map<String, Object> getComponentConfiguration() {
       //Map<String, Object> hMap = new HashMap<>();

    // TODO Auto-generated method stub

        return null;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        //esperFSOperation = new EsperFSOperation();
        Configuration config = new Configuration();
        epService = EPServiceProviderManager.getDefaultProvider(config);


        if (!epService.getEPAdministrator().getConfiguration().isEventTypeExists("weatherEvent")) {

            // Define Single-row function in ESPER
            epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("feature_selection", FeatureSelection.class.getName(), "feature_selection");


            // Create WeatherEvent using Map properties
            String createEventExp = "@EventRepresentation(objectarray) create schema weatherEvent as (prop1 Map)";
            EPStatement statement1 = epService.getEPAdministrator().createEPL(createEventExp);
            ListenerEvent listener = new ListenerEvent();
            statement1.addListener(listener);

            /*String createEventExpTest = "@EventRepresentation(map) create schema weatherTest as (prop1 Map)";
            EPStatement statementTest = epService.getEPAdministrator().createEPL(createEventExpTest);
            //ListenerEvent listenerTest = new ListenerEvent();
            statementTest.addListener(listener);*/


            // Create Batch context of 1 seconds
            /*String expression2 = "create context batch10seconds start @now end after 1 sec";
            EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
            ListenerEvent listener2 = new ListenerEvent();
            statement2.addListener(listener2);*/

            /*String expression2 = "insert into weatherTest select * from weatherEvent.win:length_batch(3)";
            EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
            //ListenerEvent listener2 = new ListenerEvent();
            statement2.addListener(listener);*/
        }

        runtime = epService.getEPRuntime();

        // Esper Query for Feature Selection using single row function which returns the map with features
        String expression = "select distinct feature_selection(e) from weatherEvent.win:length_batch(2) as e";
        EPStatement statement = epService.getEPAdministrator().createEPL(expression);
        statement.addListener(this);
        this.collector = collector;

        influx = new PushToInfluxDbBolt();

    }




    public void cleanup() {
        /*if (this.epService != null) {
            this.epService.destroy();
        }*/
    }


    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        try{
            System.out.println("event \t" + newEvents[0].getUnderlying() + "\n");
            //System.out.println("eventss \t" + newEvents[1].getUnderlying() + "\n");
            //System.out.println("old event \t" + oldEvents[0].getUnderlying() + "\n");

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.convertValue(newEvents[0].getUnderlying(), JsonNode.class);
            result_output = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
            /*Map.Entry<String,Object> entry = result_output.entrySet().iterator().next();
            //fsBolt.sendFSEventToBolt(entry.getValue());
            JsonNode node1 = mapper.convertValue(entry.getValue(), JsonNode.class);
            result_map = mapper.readValue(node1.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });*/

        }catch (IOException e){
            e.printStackTrace();
        }
        collector.emit(new Values(result_output.values()));
        //collector.emit("fsBolt", new Values(result_output.values()));
        influx.sendFSDataToDB(result_output);

    }
}