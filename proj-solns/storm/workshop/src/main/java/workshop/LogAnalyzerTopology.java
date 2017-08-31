package workshop;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class LogAnalyzerTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        BrokerHosts hosts = new ZkHosts("sandbox.hortonworks.com:2181");

        SpoutConfig sc = new SpoutConfig(hosts,
                "logs", "/logs",
                UUID.randomUUID().toString());
        sc.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout spout = new KafkaSpout(sc);

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
        mapHbase.put("hbase.rootdir", "hdfs://sandbox.hortonworks.com:8020/apps/hbase/data");
        conf.put("hbase.config", mapHbase);

        builder.setBolt("message-reassembler",
                new MessageReassemblerBolt(), 1)
                .shuffleGrouping("message-filterer");

        //set producer properties.
        Properties props = new Properties();
        props.put("bootstrap.servers", "sandbox.hortonworks.com:6667");
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

        StormSubmitter.submitTopologyWithProgressBar(
                "log-analyzer", conf,
                builder.createTopology());

        /*
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log-analyzer-local",
                conf, builder.createTopology());
        */
        
    }
}
