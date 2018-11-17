package com.foo.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author JasonLin
 * @version V1.0
 */
public class WordCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = -7753338296650445257L;
    private static final Logger LOGGER = Logger.getLogger(WordCountBolt.class);
    private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long count = this.counts.get(word);
        if (count == null) {
            count = 0L;
        }
        count++;
        counts.put(word, count);
        collector.emit(new Values(word, count));
        LOGGER.info("--word--" + word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
