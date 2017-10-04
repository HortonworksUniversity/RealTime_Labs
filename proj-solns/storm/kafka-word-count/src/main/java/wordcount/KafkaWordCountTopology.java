package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.UUID;

public class KafkaWordCountTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("sandbox.hortonworks.com:2181");

        SpoutConfig sc = new SpoutConfig(hosts,
                "sentences", "/sentences",
                UUID.randomUUID().toString());
        sc.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout spout = new KafkaSpout(sc);

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
