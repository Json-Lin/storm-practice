package com.foo.spout;

import com.foo.bolt.WordSplitBolt;
import com.foo.source.SentenceEmitter;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author JasonLin
 * @version V1.0
 */
public class SentenceSpout extends BaseRichSpout {

    private static final long serialVersionUID = -5335326175089829338L;
    private static final Logger LOGGER = Logger.getLogger(WordSplitBolt.class);

    private AtomicLong atomicLong = new AtomicLong(0);
    private SpoutOutputCollector collector;
    private SentenceEmitter sentenceEmitter;
    private ConcurrentHashMap<UUID, Values> emitted;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.sentenceEmitter = new SentenceEmitter();
        this.emitted = new ConcurrentHashMap<UUID, Values>();
    }

    @Override
    public void nextTuple() {
        //这里做了一点更改，不再在sentenceEmitter里面睡眠，这样会影响到spout线程程序发送失败的数据。
        if (atomicLong.incrementAndGet() >= 1000) {
            return;
        }
        String sentence = sentenceEmitter.emit();
        Values values = new Values(sentence);
        UUID msgId = UUID.randomUUID();

        //在spout发射的时候为每一个tuple指定一个id，这个 id 会在后续过程中用于识别 tuple
        collector.emit(values, msgId);
        //将所有发射出去的sentence记录下来，以便在失败时重新发射
        this.emitted.put(msgId, values);
    }

    /**
     * 要保证可靠性，必须实现ack和fail方法
     * 调用ack表示下游全部成功处理，此时需要从emitted移除已经ack的tuple
     *
     * @param msgId
     */
    @Override
    public void ack(Object msgId) {
        this.emitted.remove(msgId);
    }

    /**
     * 要保证可靠性，必须实现ack和fail方法
     * 调用fail表示下游某个环节处理失败，可能是程序异常，也可能是网络原因，此时需要从emitted获取失败的tuple，然后重新发送
     *
     * @param msgId
     */
    @Override
    public void fail(Object msgId) {
        Values values = this.emitted.get(msgId);
        this.collector.emit(values, msgId);
        LOGGER.info(String.format("失败重发:messageId:%s,values:%s", msgId, values));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void close() {
        super.close();
        sentenceEmitter.printCount();
    }
}
