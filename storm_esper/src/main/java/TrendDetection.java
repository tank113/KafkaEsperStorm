
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TrendDetection {

    public static Map detect_trend(Object[] lastEvent) {

        Map<String, Object> resultInc = new HashMap<String, Object>();
        Map<String, Object> resultDec = new HashMap<String, Object>();
        Map<String, Object> resultTurn = new HashMap<String, Object>();
        Map<String, Object> trend = new HashMap<String, Object>();


        Map<String, Object> first;
        Map<String, Object> second;
        Map<String, Object> third;
        System.out.println("result" + lastEvent[0]);
        //System.out.println("result1" + lastEvent[1]);
        //System.out.println("result2" + lastEvent[2]);



        first = objectToMap(lastEvent[0].toString());
        second = objectToMap(lastEvent[1].toString());
        third = objectToMap(lastEvent[2].toString());


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

    private static Map<String, Object> objectToMap(String object) {
        Map<String, Object> output=new HashMap<>();

        String[] pairs = object.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split(":");
            //System.out.println("keyvalue" + keyValue[0].trim());
            Object value = keyValue[1].trim().replace("}", "");
            //System.out.println("value" + value);
            output.put(keyValue[0].trim().replace("{", ""), value);
        }
        return output;


    }


}
