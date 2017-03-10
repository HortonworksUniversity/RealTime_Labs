package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class KafkaHBaseSentenceStoreTopology {

    public static final String TOPOLOGY_NAME = "kafka-hbase-simple";
    private static final String KAFKA_TOPIC_NAME = "lestertester";
    private static final String KAFKA_ZK_QUORUM = "zk1:2181,zk2:2181,zk3:2181";

    public static void main(String[] args) throws Exception {

        BrokerHosts hosts = new ZkHosts(KAFKA_ZK_QUORUM);
        SpoutConfig sc = new SpoutConfig(hosts,
                KAFKA_TOPIC_NAME, "/" + KAFKA_TOPIC_NAME,
                UUID.randomUUID().toString());
        sc.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout spout = new KafkaSpout(sc);

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka_spout", spout, 1);

        builder.setBolt(SimpleIDBolt.BOLT_NAME,
                new SimpleIDBolt(), 1)
                .shuffleGrouping("kafka_spout");

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField(SimpleIDBolt.EMIT_BOGUS_KEY)
                .withColumnFields(new Fields(SimpleIDBolt.EMIT_SENTENCE))
                .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("bogus_table", mapper)
                .withConfigKey("hbase.config");

        builder.setBolt("hbase-bolt", hbase, 1)
                .shuffleGrouping(SimpleIDBolt.BOLT_NAME);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);


        Map<String, Object> mapHbase = new HashMap<String, Object>();
        //mapHbase.put("storm.keytab.file", "/etc/security/keytabs/storm.headless.keytab");
        //mapHbase.put("storm.kerberos.principal", "storm-telus_training");
        mapHbase.put("hbase.rootdir", "hdfs://FedExNS/apps/hbase/data");
        conf.put("hbase.config", mapHbase);





        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

/*
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME,
                conf, builder.createTopology());
*/


    }
}
