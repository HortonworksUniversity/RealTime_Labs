package wordcount;


import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentenceBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //tuple has five fields; topic, partition, offset, key, value
        System.out.println("\n*** Current message *** " + tuple.toString());

        String[] words = StringUtils.split(tuple.getStringByField("value"));
        for( String word : words ) {
            System.out.println("\n*** Split Sentence Bolt *** " + word + " ***\n");
            basicOutputCollector.emit(new Values(word));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
