package workshop;


import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MessageReassemblerBolt extends BaseBasicBolt {


    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String ipAddress = tuple.getStringByField("ip-address");
        StringBuffer sb = new StringBuffer();
        sb.append(ipAddress);
        sb.append("\t");
        sb.append(tuple.getStringByField("type"));
        sb.append("\t");
        sb.append(tuple.getStringByField("details"));

        basicOutputCollector.emit(new Values(ipAddress, sb.toString()));

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                "ip-address", "delimited-record"));
    }
}
