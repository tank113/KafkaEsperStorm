import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FeatureSelection {

    public static Map feature_selection(Object[] lastEvent) {

        final Map<String, Object> result = new HashMap<String, Object>();
        Map<String, Object> first;
        Map<String, Object> second;
        System.out.println("result" + lastEvent[0]);
        //System.out.println("result1" + lastEvent[1]);
        //System.out.println("result2" + lastEvent[2]);



        first = objectToMap(lastEvent[0].toString());
        second = objectToMap(lastEvent[1].toString());
        //Map third = objectToMap(lastEvent[2]);
        //System.out.println("resultMap" + first);

        try{
            for (Object k : first.keySet())
            {
                //System.out.println("check" + first.get(k).toString().replace("\"", ""));
                //System.out.println("checks" + second.get(k).toString().replace("\"", ""));
                String firstS = first.get(k).toString().replace("\"", "");
                String lastS = second.get(k).toString().replace("\"", "");
                Double firstVal = new Double(firstS);
                Double lastVal = new Double(lastS);
                if((lastVal - firstVal)>0 || (lastVal - firstVal)<0){
                    result.put(k.toString(), first.get(k));
                    result.put(k.toString(), second.get(k));
                }
            }
            //System.out.println("result" + result);
        } catch (NullPointerException np) {
            np.printStackTrace();
        }
        System.out.println("resultMap" + result);
        return result;
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
