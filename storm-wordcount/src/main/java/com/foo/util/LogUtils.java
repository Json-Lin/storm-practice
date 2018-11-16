package com.foo.util;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * @author JasonLin
 * @version V1.0
 * @date 2018/11/16
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
            if (strs.length > 1) {
                result.put(strs[0], strs[1]);
            } else {
                result.put(strs[0], "");
            }
        }
        for (String key : result.keySet()) {
            System.out.println(key + " : " + result.get(key));
        }
    }

    public static void main(String[] args) throws IOException {
        sysoWordByWhatThread("D:\\work-count.log", "--word--", "--word--");
        //sysoWordByWhatThread("D:\\work-count.log", "--sentence--", "--sentence--");
    }
}
