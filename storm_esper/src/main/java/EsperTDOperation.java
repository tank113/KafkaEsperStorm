import com.espertech.esper.client.*;

import java.util.Map;

public class EsperTDOperation {

    private EPRuntime runtime = null;
    public EsperTDOperation() {
        //Start processing messages
        Configuration config = new Configuration();
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(config);

        // Define Single-row function in ESPER
        epService.getEPAdministrator().getConfiguration().addPlugInSingleRowFunction("detect_trend", TrendDetection.class.getName(), "detect_trend");


        // Create WeatherTrendEvent using Map properties
        String createEventExpTrends = "@EventRepresentation(map) create schema weatherTrendEvent as (prop1 Map)";
        EPStatement statementTrend = epService.getEPAdministrator().createEPL(createEventExpTrends);
        ListenerEvent listenerForTrend = new ListenerEvent();
        statementTrend.addListener(listenerForTrend);

        // Create Batch context of 1 seconds
        String expression2 = "create context batch10seconds start @now end after 1 sec";
        EPStatement statement2 = epService.getEPAdministrator().createEPL(expression2);
        ListenerEvent listener2 = new ListenerEvent();
        statement2.addListener(listener2);

        EPRuntime runtime = epService.getEPRuntime();

        // Esper Query for Trends detection using single row function which returns the nested map with increasing, decreasing and Turn trends features.
        String expressionTrend = "select detect_trend(trendEvent, prev(trendEvent), first(trendEvent)) from weatherTrendEvent.win:length(3) as trendEvent";
        EPStatement statement = epService.getEPAdministrator().createEPL(expressionTrend);
        statement.addListener(new CEPListener());

    }

    public static class CEPListener implements UpdateListener {
        public void update(EventBean[] newEvents, EventBean[] oldEvents) {
            System.out.println("event \t" + newEvents[0].getUnderlying() + "\n");
            System.out.println("old event \t" + oldEvents[0].getUnderlying() + "\n");

        }
    }

    public void esperPutTD(Map tdEvent){

        runtime.sendEvent(tdEvent, "weatherTrendEvent");
    }

    public static void main(String[] s) throws InterruptedException
    {
        EsperTDOperation esperFSOperation = new EsperTDOperation();
        // We generate a few ticks...

        Thread.sleep(200000);
    }
}
