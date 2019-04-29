package workshop;


import org.apache.storm.shade.org.apache.commons.lang.ArrayUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MessageFiltererBolt extends BaseBasicBolt {

    public static final String[] INCIDENT_TYPES = {"WARN", "ERROR"};

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String messageType = tuple.getStringByField("message-type");

        if(ArrayUtils.contains( INCIDENT_TYPES, messageType ) ) {
            System.out.println("\n*** Message Filter Bold >>IS<< emitting message-type of " +
                    messageType + " ***\n");
            basicOutputCollector.emit(new Values(
                    tuple.getStringByField("ip-address"),
                    messageType,
                    tuple.getStringByField("message-details"),
                    1));  //this value will be used for HBase counter col identifier
        } else {
            System.out.println("\n*** Message Filter Bold >>NOT<< emitting message-type of " +
                messageType + " ***\n");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                "ip-address", "type", "details", "total"));
    }
}
