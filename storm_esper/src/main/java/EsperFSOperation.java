import com.espertech.esper.client.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EsperFSOperation {

    private EPRuntime runtime = null;
    private  FeatureSelectionBolt fsBolt = new FeatureSelectionBolt();
    private Object[] objArr = new Object[5];
    private int count=0;
    public EsperFSOperation() {
        //Start processing messages
        Configuration config = new Configuration();
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);


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
            statement.addListener(new UpdateListener() {
                @Override
                public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                    try{
                        System.out.println("event \t" + newEvents[0].getUnderlying() + "\n");
                        //System.out.println("eventss \t" + newEvents[1].getUnderlying() + "\n");
                        //System.out.println("old event \t" + oldEvents[0].getUnderlying() + "\n");
                        Map<String, Object> result_output = new HashMap<>();
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode node = mapper.convertValue(newEvents[0].getUnderlying(), JsonNode.class);
                        result_output = mapper.readValue(node.toString(),
                                new TypeReference<HashMap<String, Object>>() {
                                    });
                        Map.Entry<String,Object> entry = result_output.entrySet().iterator().next();
                        //fsBolt.sendFSEventToBolt(entry.getValue());

                    }catch (IOException e){
                        e.printStackTrace();
                    }


                }
            });


    }



    public void esperPutFS(Object[] fsEventArr){

        runtime.sendEvent(fsEventArr, "weatherEvent");

    }

    public void createObjectArray(Object fsEvent){

        if(count<3){
            objArr[count++]=fsEvent;
        }
        else if(count==3){
            esperPutFS(objArr);
            count=0;
        }



    }

    public static void main(String[] s) throws InterruptedException
    {
        EsperFSOperation esperFSOperation = new EsperFSOperation();
        // We generate a few ticks...

        Thread.sleep(200000);
    }
}
