package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;


public class RandomWordCountTopology {

    public static final String TOPOLOGY_NAME = "word-count";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RandomSentenceSpout.SPOUT_NAME,
                new RandomSentenceSpout(),1);

        builder.setBolt(SplitSentenceBolt.BOLT_NAME,
                new SplitSentenceBolt(), 2)
                .shuffleGrouping(RandomSentenceSpout.SPOUT_NAME);

        builder.setBolt(WordCountBolt.BOLT_NAME,
                new WordCountBolt(), 4)
                .fieldsGrouping(SplitSentenceBolt.BOLT_NAME,
                        new Fields(SplitSentenceBolt.EMIT_WORD));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);

        /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        */


        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME,
                conf, builder.createTopology());


    }
}
