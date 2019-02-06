import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FeatureSelection {

    public static Map feature_selection(Map firstEvent, Map lastEvent) {

        final Map<String, Object> result = new HashMap<String, Object>();



        try{
            for (Object k : lastEvent.keySet())
            {
                Double first = new Double(firstEvent.get(k).toString());
                Double last = new Double(lastEvent.get(k).toString());
                if((last - first)>0 || (last - first)<0){
                    result.put(k.toString(), firstEvent.get(k));
                    result.put(k.toString(), lastEvent.get(k));
                }
            }
        } catch (NullPointerException np) {
            np.printStackTrace();
        }
        return result;
    }

}
