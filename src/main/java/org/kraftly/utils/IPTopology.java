package org.kraftly.utils;

/**
 * Created by kunal on 4/3/17.
 */
import kafka.api.OffsetRequest;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.log4j.Logger;
import org.kraftly.utils.bolts.IntermediateRankingsBolt;
import org.kraftly.utils.bolts.RollingCountBolt;
import org.kraftly.utils.bolts.RollingCountAggBolt;
import org.kraftly.utils.bolts.TotalRankingsBolt;
import org.kraftly.utils.util.Constants;

import java.util.ArrayList;
import java.util.List;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter. It takes an approach that assumes that some works will be much
 * more common then other words, and uses partialKeyGrouping to better balance the skewed load.
 */
public class IPTopology {
    private static final Logger LOG = Logger.getLogger(IPTopology.class);
    private static final int TOP_N = 5;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;

    public IPTopology(String topologyName) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();

        wireTopology();
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology() throws InterruptedException {
        String counterId = "counter";
        String aggId = "aggregator";
        String intermediateRankerId = "intermediateRanker";
        String totalRankerId = "finalRanker";
        String zkConnString = Constants.ZOOKEEPER_HOST + ":" + Constants.ZOOKEEPER_PORT;
        String topic = Constants.SOURCE_TOPIC;
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic,
                Constants.ID);
        List<String> server = new ArrayList<String>();
        server.add(Constants.ZOOKEEPER_HOST);
        kafkaSpoutConfig.startOffsetTime = OffsetRequest.LatestTime();
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpoutConfig.zkServers = server;
        kafkaSpoutConfig.zkPort = Constants.ZOOKEEPER_PORT;
        kafkaSpoutConfig.zkRoot = "/" + topic;
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig),5);
        builder.setBolt(counterId, new RollingCountBolt(300, 10), 4).partialKeyGrouping("kafka-spout", new Fields("str"));
        builder.setBolt(aggId, new RollingCountAggBolt(), 4).fieldsGrouping(counterId, new Fields("obj"));
        builder.setBolt(intermediateRankerId, new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(aggId, new Fields("obj"));
        builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
    }



    public void runLocally() throws InterruptedException {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, topologyConfig, builder.createTopology());
    }

    public void runRemotely() throws Exception {
        StormSubmitter.submitTopologyWithProgressBar(topologyName, topologyConfig, builder.createTopology());
    }

    public static void main(String[] args) throws Exception {
        String topologyName = "slidingWindowCounts";
        if (args.length >= 1) {
            topologyName = args[0];
        }
        boolean runLocally = true;
        if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
            runLocally = false;
        }

        LOG.info("Topology name: " + topologyName);
        IPTopology rtw = new IPTopology(topologyName);
        if (runLocally) {
            LOG.info("Running in local mode");
            rtw.runLocally();
        }
        else {
            LOG.info("Running in remote (cluster) mode");
            rtw.runRemotely();
        }
    }
}
