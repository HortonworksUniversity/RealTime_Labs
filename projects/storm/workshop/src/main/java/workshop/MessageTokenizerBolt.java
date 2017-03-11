package workshop;


import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MessageTokenizerBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String[] logElements = StringUtils.split(tuple.getString(0), '\t');

        //for our simple use case, ignore date and time although...
        String ipAddress      = logElements[2];
        String messageType    = logElements[3];
        String messageDetails = logElements[4];
        System.out.println("\n*** Message Tokenizer Bolt [ " +
                ipAddress      + " | " +
                messageType    + " | " +
                messageDetails + " ] ***\n");;

        //to make our problem easier, just leave date and time out of the emitted tuple
        basicOutputCollector.emit(new Values(ipAddress, messageType, messageDetails));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ip-address", "message-type", "message-details"));
    }
}
