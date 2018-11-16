package com.foo.bolt;

import com.google.common.collect.Lists;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

/**
 * @author JasonLin
 * @version V1.0
 */
public class WordSplitBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2932049413480818649L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word : words) {
            collector.emit(new Values(word));
        }
        System.out.println(Thread.currentThread().getName() + "--sentence--" + sentence);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
