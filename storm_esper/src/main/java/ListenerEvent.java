import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListenerEvent implements UpdateListener {

    final static Logger logger = LoggerFactory.getLogger(UpdateListener.class);

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        EventBean event = newEvents[0];
        Object average = event.get("hum");
        System.out.println("uponval of the average stood at"+ event);
    }
}
