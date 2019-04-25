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
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

public class LogAnalyzerTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();

        String namenodeServer = "replaceWith-INTERNAL-dns-of-NAMENODESERVER-node";

        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);


        Map<String, Object> mapHbase = new HashMap<String, Object>();
        mapHbase.put("hbase.rootdir", "hdfs://" + namenodeServer + ":8020/apps/hbase/data");
        conf.put("hbase.config", mapHbase);


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
