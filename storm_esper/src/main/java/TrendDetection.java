
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TrendDetection {

    public static Map detect_trend(Map firstEvent, Map prevEvent, Map lastEvent) {


        Map<String, Object> resultInc = new HashMap<String, Object>();
        Map<String, Object> resultDec = new HashMap<String, Object>();
        Map<String, Object> resultTurn = new HashMap<String, Object>();
        Map<String, Object> trend = new HashMap<String, Object>();

        // Convert Object to Map
        Map first = objectToMap(firstEvent);
        Map second = objectToMap(prevEvent);
        Map third = objectToMap(lastEvent);


        for (Object k : first.keySet()){
            Double firstVal = new Double(first.get(k).toString());
            Double secVal = new Double(second.get(k).toString());
            Double lastVal = new Double(third.get(k).toString());

            // It will be decreasing because first is the recent event and third is the last event in the window (See example in notes)
            if((lastVal>secVal) && (secVal>firstVal)){
                resultInc.put(k.toString(), third.get(k));
                trend.put("Decreasing", resultInc);
            }
            else if((lastVal<secVal) && (secVal<firstVal)){
                resultDec.put(k.toString(), third.get(k));
                trend.put("Increasing", resultDec);
            }
            else{
                resultTurn.put(k.toString(), second.get(k));
                trend.put("Turn", resultTurn);
            }

        }

        return trend;
    }

    public static Map objectToMap(Map<String, Object> result_output) {
        Map<String, Object> output = new HashMap<String, Object>();
        try{
            Map.Entry<String, Object> entry = result_output.entrySet().iterator().next();
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.convertValue(entry.getValue(), JsonNode.class);
            output = mapper.readValue(node.toString(),
                    new TypeReference<HashMap<String, Object>>() {
                    });
        }catch (IOException e){

        }

        return output;


    }


}
