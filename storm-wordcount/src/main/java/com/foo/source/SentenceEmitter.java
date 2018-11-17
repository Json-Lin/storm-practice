package com.foo.source;


import com.google.common.util.concurrent.AtomicLongMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author JasonLin
 * @version V1.0
 */
public class SentenceEmitter {
    private AtomicLong atomicLong = new AtomicLong(0);

    private final AtomicLongMap<String> CONUTS = AtomicLongMap.create();

    private final String[] SENTENCES = {"The logic for a realtime application is packaged into a Storm topology",
            " A Storm topology is analogous to a MapReduce job ",
            "One key difference is that a MapReduce job eventually finishes ",
            "whereas a topology runs forever or until you kill it of course ",
            "A topology is a graph of spouts and bolts that are connected with stream groupings"};


    /**
     * 随机发射sentence，并记录单词数量，该统计结果用于验证与storm的统计结果是否相同。
     * 当发射总数<1000时，停止发射，以便程序在停止时，其它bolt能将发射的数据统计完毕
     *
     * @return
     */
    public String emit() {
        if (atomicLong.incrementAndGet() >= 1000) {
            try {
                Thread.sleep(10000 * 1000);
            } catch (InterruptedException e) {
                return null;
            }
        }
        int randomIndex = (int) (Math.random() * SENTENCES.length);
        String sentence = SENTENCES[randomIndex];
        for (String s : sentence.split(" ")) {
            CONUTS.incrementAndGet(s);
        }
        return sentence;
    }

    public void printCount() {
        System.out.println("--- Emitter COUNTS ---");
        List<String> keys = new ArrayList<String>();
        keys.addAll(CONUTS.asMap().keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + " : " + this.CONUTS.get(key));
        }
        System.out.println("--------------");
    }

    public AtomicLongMap<String> getCount() {
        return CONUTS;
    }

    public static void main(String[] args) {
        SentenceEmitter sentenceEmitter = new SentenceEmitter();
        for (int i = 0; i < 20; i++) {
            System.out.println(sentenceEmitter.emit());
        }
        sentenceEmitter.printCount();
    }
}
