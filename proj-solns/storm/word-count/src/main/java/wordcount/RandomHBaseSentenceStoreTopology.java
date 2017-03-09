package wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


public class RandomHBaseSentenceStoreTopology {

    public static final String TOPOLOGY_NAME = "kafka-word-count";
    private static final String KAFKA_TOPIC_NAME = "lestertester";
    private static final String KAFKA_HOST_NAME = "kafka";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(RandomSentenceIDSpout.SPOUT_NAME,
                new RandomSentenceIDSpout(),1);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField(RandomSentenceIDSpout.EMIT_BOGUS_KEY)
                .withColumnFields(new Fields(RandomSentenceIDSpout.EMIT_SENTENCE))
                .withColumnFamily("cf");

        HBaseBolt hbase = new HBaseBolt("bogus_table", mapper)
                .withConfigKey("hbase.config");

        builder.setBolt("hbase-bolt", hbase, 1)
                .shuffleGrouping(RandomSentenceIDSpout.SPOUT_NAME);

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);


        Map<String, Object> mapHbase = new HashMap<String, Object>();
        //mapHbase.put("storm.keytab.file", "/etc/security/keytabs/storm.headless.keytab");
        //mapHbase.put("storm.kerberos.principal", "storm-telus_training");
        mapHbase.put("hbase.rootdir", "hdfs://sandbox.hortonworks.com:8020/apps/hbase/data");
        conf.put("hbase.config", mapHbase);



        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());


/*
        StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME,
                conf, builder.createTopology());
*/


    }
}