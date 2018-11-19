package com.foo.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author JasonLin
 * @version V1.0
 */
public class ExceptionWordSplitBolt extends BaseRichBolt {
    private static final long serialVersionUID = 2932049413480818649L;
    private static final Logger LOGGER = Logger.getLogger(ExceptionWordSplitBolt.class);
    private OutputCollector collector;

    private AtomicInteger atomicInteger = new AtomicInteger(1);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            atomicInteger.getAndIncrement();
            String sentence = input.getStringByField("sentence");
            String[] words = sentence.split(" ");
            if (atomicInteger.get() == 20 || atomicInteger.get() == 200) {
                throw new RuntimeException("模拟异常情况，sentence:" + sentence);
            }
            for (String word : words) {
                collector.emit(new Values(word));
            }
            LOGGER.info("--sentence--" + sentence);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
