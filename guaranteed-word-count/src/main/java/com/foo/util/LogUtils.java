package com.foo.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * @author JasonLin
 * @version V1.0
 */
public class LogUtils {

    public static void sysoWordByWhatThread(String fileName, String prefix, String splitPointStr) throws IOException {
        List<String> strings = FileUtils.readFromFileToList(fileName);
        List<String> need = Lists.newArrayList();
        for (String s : strings) {
            if (!s.contains(prefix)) {
                continue;
            }
            need.add(s);
        }
        HashMultimap<String, String> result = HashMultimap.create();
        for (String n : need) {
            String[] strs = n.split(splitPointStr);
            String strs2 = strs[0].split("com.foo.bolt")[0].split("] \\[Thread")[1];
            if (strs.length > 1) {
                result.put("[Thread" + strs2, strs[1]);
            } else {
                result.put("[Thread" + strs2, "");
            }
        }
        for (String key : result.keySet()) {
            System.out.println(key + " : " + result.get(key));
        }
    }

    public static void main(String[] args) throws IOException {
        sysoWordByWhatThread("D:\\idea-workspace\\local\\logs\\storm-wordcount-2018-11-18.0.log", "--word--", "--word--");
        sysoWordByWhatThread("D:\\idea-workspace\\local\\logs\\storm-wordcount-2018-11-18.0.log", "--sentence--", "--sentence--");
        sysoWordByWhatThread("D:\\idea-workspace\\local\\logs\\storm-wordcount-2018-11-18.0.log", "--globalreport--", "--globalreport--");
    }
}
