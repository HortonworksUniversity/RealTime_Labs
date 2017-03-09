package wordcount;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;


public class WordCountBolt extends BaseBasicBolt {

    public static final String BOLT_NAME = "counter";
    public static final String EMIT_WORD = "word";
    public static final String EMIT_COUNT = "count";

    private Map<String, Integer> counts = new HashMap<String, Integer>();

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getStringByField(SplitSentenceBolt.EMIT_WORD);
        Integer count = counts.get(word);
        if( count == null ) {
            count = 0;
        }
        count++;
        counts.put(word, count);
        System.out.println("\n******* Word Count Bolt ***** " + word + " = " + count + " ******\n");
        basicOutputCollector.emit(new Values(word, count));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(EMIT_WORD, EMIT_COUNT));
    }

}