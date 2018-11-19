package com.foo.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Description:
 * </p>
 *
 * @author JiaSonglin
 * @version V1.0, 2017年4月6日 上午9:53:47
 */
public class FileUtils {

    public static void main(String[] args) throws IOException {
        readExcludeToFile("C:/log/all.txt", "C:/log/sub.txt", "C:/log/diff.txt");
    }

    /**
     * 以行读取文件
     *
     * @param fileName
     * @return 一行为一个String的List
     * @throws IOException
     */
    public static List<String> readFromFileToList(String fileName) throws IOException {
        List<String> list = new ArrayList<>();
        Reader in = null;
        BufferedReader reader = null;
        try {
            in = new FileReader(fileName);
            reader = new BufferedReader(in);
            String line;
            while (null != (line = reader.readLine())) {
                list.add(line);
            }
            return list;
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            if (null != in) {
                in.close();
            }
            if (null != reader) {
                reader.close();
            }
        }
    }

    /**
     * 将list写入文件，list的每个元素作为一行
     *
     * @param list
     * @param writeFile
     * @throws IOException
     */
    public static void writeFromListToFile(List<String> list, String writeFile) throws IOException {
        Writer writer = null;
        BufferedWriter bufferedWriter = null;
        try {
            writer = new FileWriter(writeFile);
            bufferedWriter = new BufferedWriter(writer);
            for (String str : list) {
                bufferedWriter.write(str);
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            throw new IOException(e);
        } finally {
            if (null != writer) {
                writer.close();
            }
            if (null != bufferedWriter) {
                bufferedWriter.close();
            }
        }
    }

    /**
     * 从源文件里面去掉排除文件里面的数据
     *
     * @param source  源文件
     * @param exclude 排除文件
     * @return
     * @throws IOException
     */
    public static List<String> readExcludeToList(String source, String exclude) throws IOException {
        List<String> fileSource = readFromFileToList(source);
        List<String> fileListExclude = readFromFileToList(exclude);
        for (String str : fileListExclude) {
            if (fileSource.contains(str)) {
                fileSource.remove(str);
            }
        }
        return fileSource;
    }

    public static void readExcludeToFile(String source, String exclude, String write) throws IOException {
        List<String> excludeList = readExcludeToList(source, exclude);
        writeFromListToFile(excludeList, write);
    }
}
