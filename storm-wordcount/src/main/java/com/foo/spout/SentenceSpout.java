package com.foo.spout;

import com.foo.source.WordEmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author JasonLin
 * @version V1.0
 * @date 2018/11/13
 */
public class SentenceSpout extends BaseRichSpout {

    private static final long serialVersionUID = -5335326175089829338L;
    private SpoutOutputCollector collector;
    private WordEmitter wordEmitter;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.wordEmitter = new WordEmitter();
    }

    @Override
    public void nextTuple() {
        String word = wordEmitter.emit();
        collector.emit(new Values(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void close() {
        super.close();
        wordEmitter.printCount();
    }
}
