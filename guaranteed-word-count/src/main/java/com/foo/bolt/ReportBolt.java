package com.foo.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReportBolt extends BaseRichBolt {
    private static final Logger LOGGER = Logger.getLogger(ReportBolt.class);

    private static final long serialVersionUID = -3973016696731250995L;
    private HashMap<String, Long> counts = null;
    private OutputCollector collector;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        counts = new HashMap<>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long count = input.getLongByField("count");
        this.counts.put(word, count);
        //当处理成功时ack该Tuple
        collector.ack(input);
        LOGGER.info("--globalreport--" + word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("--------------");
    }
}
