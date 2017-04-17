package org.kraftly.utils.bolts;

/**
 * Created by kunal on 4/3/17.
 */
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt aggregates counts from multiple upstream bolts.
 */
public class RollingCountAggBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(RollingCountAggBolt.class);
    //Mapping of key->upstreamBolt->count
    private Map<Object, Map<Integer, Long>> counts = new HashMap<Object, Map<Integer, Long>>();
    private OutputCollector collector;


    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Object obj = tuple.getValue(0);
        long count = tuple.getLong(1);
        int source = tuple.getSourceTask();
        Map<Integer, Long> subCounts = counts.get(obj);
        if (subCounts == null) {
            subCounts = new HashMap<Integer, Long>();
            counts.put(obj, subCounts);
        }
        //Update the current count for this object
        subCounts.put(source, count);
        //Output the sum of all the known counts so for this key
        long sum = 0;
        for (Long val: subCounts.values()) {
            sum += val;
        }
        collector.emit(new Values(obj, sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("obj", "count"));
    }
}