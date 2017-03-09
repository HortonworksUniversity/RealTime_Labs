package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.UUID;


public class KafkaWordCountTopology {

    public static final String TOPOLOGY_NAME = "kafka-word-count";
    private static final String KAFKA_TOPIC_NAME = "test2";
    private static final String KAFKA_HOST_NAME = "sandbox.hortonworks.com";

    public static void main(String[] args) throws Exception {

        BrokerHosts hosts = new ZkHosts(KAFKA_HOST_NAME + ":2181");
        SpoutConfig sc = new SpoutConfig(hosts,
                KAFKA_TOPIC_NAME, "/" + KAFKA_TOPIC_NAME,
                UUID.randomUUID().toString());
        sc.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout spout = new KafkaSpout(sc);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka_spout", spout, 1);

        builder.setBolt(SimpleBolt.BOLT_NAME,
                new SimpleBolt(), 1)
                .shuffleGrouping("kafka_spout");

        builder.setBolt(SplitSentenceBolt.BOLT_NAME,
                new SplitSentenceBolt(), 2)
                .shuffleGrouping(SimpleBolt.BOLT_NAME);

        builder.setBolt(WordCountBolt.BOLT_NAME,
                new WordCountBolt(), 4)
                .fieldsGrouping(SplitSentenceBolt.BOLT_NAME,
                        new Fields(SplitSentenceBolt.EMIT_WORD));

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(3);


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());


/*
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME,
                conf, builder.createTopology());
*/


    }
}