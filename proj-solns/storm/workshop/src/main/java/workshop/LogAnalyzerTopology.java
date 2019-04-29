package workshop;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class LogAnalyzerTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        
        String namenodeServer = "replaceWith-INTERNAL-dns-of-NAMENODESERVER-node";

        String bootstrapServer = "replaceWith-INTERNAL-dns-of-ONE-broker-node";

        KafkaSpout spout = new KafkaSpout<>(KafkaSpoutConfig.builder(
                bootstrapServer + ":6667",
                "logs").build());

        builder.setSpout("log-spout", spout, 1);

        builder.setBolt("message-tokenizer",
                new MessageTokenizerBolt(), 1)
                .shuffleGrouping("log-spout");

        builder.setBolt("message-filterer",
                new MessageFiltererBolt(), 1)
                .shuffleGrouping("message-tokenizer");


        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("ip-address")
                .withColumnFields(new Fields("type", "details"))
                .withCounterFields(new Fields("total"))
                .withColumnFamily("event");

        HBaseBolt hbase = new HBaseBolt("incident", mapper)
                .withConfigKey("hbase.config");

        builder.setBolt("hbase-bolt", hbase, 1)
                .shuffleGrouping("message-filterer");


        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);


        Map<String, Object> mapHbase = new HashMap<String, Object>();
        mapHbase.put("hbase.rootdir", "hdfs://" + namenodeServer + ":8020/apps/hbase/data");
        conf.put("hbase.config", mapHbase);

        builder.setBolt("message-reassembler",
                new MessageReassemblerBolt(), 1)
                .shuffleGrouping("message-filterer");

        //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer + ":6667");
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //"ip-address", "delimited-record"));

        KafkaBolt kafkaBolt = new KafkaBolt()
                .withProducerProperties(props)
                .withTopicSelector(new DefaultTopicSelector("bolt"))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(
                        "ip-address", "delimited-record"));
        builder.setBolt("kafka-bolt", kafkaBolt, 1).shuffleGrouping("message-reassembler");

        /*
        StormSubmitter.submitTopologyWithProgressBar(
                "log-analyzer", conf,
                builder.createTopology());
        */

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log-analyzer-local",
                conf, builder.createTopology());

    }
}
