package wordcount;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class SimpleIDBolt extends BaseBasicBolt {

    public static final String BOLT_NAME = "simple";
    public static final String EMIT_SENTENCE = "sentence";
    public static final String EMIT_BOGUS_KEY = "BOGUS";

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try{
            //String message = new String((byte[]) tuple.getValue(0));
            String message = tuple.getString(0);
            System.out.println("\n******* Simple Bolt ***** " + message + " ******\n");
            basicOutputCollector.emit(new Values(EMIT_BOGUS_KEY, message));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(EMIT_BOGUS_KEY, EMIT_SENTENCE));
    }

}
