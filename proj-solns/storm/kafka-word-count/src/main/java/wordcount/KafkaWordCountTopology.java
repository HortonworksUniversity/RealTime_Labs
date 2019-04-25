package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class KafkaWordCountTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        String bootstrapServer = "replaceWith-INTERNAL-dns-of-ONE-broker-node";

        KafkaSpout spout = new KafkaSpout<>(KafkaSpoutConfig.builder(
                bootstrapServer + ":6667",
                "sentences").build());

        builder.setSpout("kafka-spout", spout, 1);

        builder.setBolt("splitter",
                new SplitSentenceBolt(), 1)
                .shuffleGrouping("kafka-spout");

        builder.setBolt("counter",
                new WordCountBolt(), 1)
                .fieldsGrouping("splitter", new Fields("word"));


        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);

        /*
        StormSubmitter.submitTopologyWithProgressBar(
                "word-count", conf,
                builder.createTopology());
        */

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count-local-soln",
                conf, builder.createTopology());

    }
}
