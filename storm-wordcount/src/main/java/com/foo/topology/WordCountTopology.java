package com.foo.topology;


import com.foo.bolt.ReportBolt;
import com.foo.bolt.WordCountBolt;
import com.foo.bolt.WordSplitBolt;
import com.foo.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {

        SentenceSpout spout = new SentenceSpout();
        WordSplitBolt splitBolt = new WordSplitBolt();
        WordCountBolt countBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SENTENCE_SPOUT_ID, spout);
        //将生成的sentence随机分组,然后发射出去
        builder.setBolt(SPLIT_BOLT_ID, splitBolt,3) .shuffleGrouping(SENTENCE_SPOUT_ID);

        //splitBolt按照空格后分隔sentence为word，然后发射给countBolt
        builder.setBolt(COUNT_BOLT_ID, countBolt, 3).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word")).setNumTasks(6);

        // WordCountBolt --> ReportBolt
        builder.setBolt(REPORT_BOLT_ID, reportBolt,2).globalGrouping(COUNT_BOLT_ID);

        Config config = new Config();
        config.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        Thread.sleep(15*1000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
