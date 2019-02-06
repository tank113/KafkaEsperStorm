import com.espertech.esper.client.*;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class EsperFSOperation {

    private EPRuntime runtime = null;
    public EsperFSOperation() {
        //Start processing messages
        Configuration config = new Configuration();
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);

        // Define Single-row function in ESPER
        epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("feature_selection", FeatureSelection.class.getName(), "feature_selection");


        // Create WeatherEvent using Map properties
        String createEventExp = "@EventRepresentation(map) create schema weatherEvent as (prop1 Map)";
        EPStatement statement1 = epService.getEPAdministrator().createEPL(createEventExp);
        ListenerEvent listener = new ListenerEvent();
        statement1.addListener(listener);


        // Create Batch context of 1 seconds
        String expression2 = "create context batch10seconds start @now end after 1 sec";
        EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
        ListenerEvent listener2 = new ListenerEvent();
        statement2.addListener(listener2);

        EPRuntime runtime = epService.getEPRuntime();

        // Esper Query for Feature Selection using single row function which returns the map with features
        String expression = "select distinct feature_selection(first(e), last(e)) from weatherEvent.win:length(3) as e";
        EPStatement statement = epService.getEPAdministrator().createEPL(expression);
        statement.addListener(new CEPListener());

    }

    public static class CEPListener implements UpdateListener {
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            System.out.println("event \t" + newEvents[0].getUnderlying() + "\n");
            System.out.println("old event \t" + oldEvents[0].getUnderlying() + "\n");

        }
    }

    public void esperPutFS(Map<String, Object> fsEvent){

        runtime.sendEvent(fsEvent, "weatherEvent");
    }


    public static void main(String[] s) throws InterruptedException
    {
        EsperFSOperation esperFSOperation = new EsperFSOperation();
        // We generate a few ticks...

        Thread.sleep(200000);
    }
}
