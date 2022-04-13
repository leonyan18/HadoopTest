package com.yan.hadoop.reducer;

import com.yan.hadoop.client.HdfsClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author yan
 */
public class CalculateReducer extends Reducer<Text, Text, Text, Text> {
    public static HdfsClient hdfsClient;
    public static String outputPath;
    public static final String PART = "/part-r-00000";
    public static final String WORD_COUNT = "Word_Count_";
    public static final String TF_COUNT = "TF_Count";
    public static Integer size=0;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        hdfsClient = new HdfsClient(configuration.get("fs.defaultFS"), configuration);
        outputPath = configuration.get("output_path");
        size = Integer.valueOf(configuration.get("document_size"));
        try {
            hdfsClient.init();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] files = new String[5];
        int i = 0;
        for (Text line : values) {
            files[i] = line.toString();
            i++;
        }
        double dis = calculate(files[0], files[1]);
        System.out.println(files[0] + "与" + files[1] + "的相似度为：" + dis);
        context.write(new Text(files[0] + "-" + files[1]), new Text(dis + "\r\n"));
        System.out.println("------------------------");
    }

    public static void getWordCount(Map<String, Integer> m, String s) {
        s = hdfsClient.readFileContext(outputPath + WORD_COUNT + s + PART);
        String[] maps = s.split("\n");
        for (String t : maps) {
            if (StringUtils.isEmpty(t)) {
                continue;
            }
            String[] temp = t.split("\t");
            m.put(temp[0], Integer.parseInt(temp[1]));
        }
    }

    public static void getTFCount(Map<String, Integer> m) {
        String s = hdfsClient.readFileContext(outputPath + TF_COUNT + PART);
        String[] maps = s.split("\n");
        for (String t : maps) {
            if (StringUtils.isEmpty(t)) {
                continue;
            }
            String[] temp = t.split("\t");
            m.put(temp[0], Integer.parseInt(temp[1]));
        }
    }

    //计算string1和string2的余弦值
    public static double calculate(String s1, String s2) {
        //以键值形式存储m1和m2的值
        HashMap<String, Integer> m1 = new HashMap<>(1000);
        HashMap<String, Integer> m2 = new HashMap<>(1000);
        HashMap<String, Integer> tf = new HashMap<>(1000);
        getWordCount(m1, s1);
        getWordCount(m2, s2);
        getTFCount(tf);
        HashSet<String> set = new HashSet<>(m1.size() + m2.size());
        //set为两个行的并集的key值
        set.addAll(m1.keySet());
        set.addAll(m2.keySet());
        double n1 = 0, n2 = 0, nfinal = 0;
        for (String st : set) {
            double tfidf=size/getDoubleVal(tf.get(st));
            double val1 = getDoubleVal(m1.get(st))/m1.size()*tfidf;
            double val2 = getDoubleVal(m2.get(st))/m2.size()*tfidf;
            n1 += val1 * val1;
            n2 += val2 * val2;
            nfinal += val1 * val2;
        }
        System.out.println(nfinal + "与" + n1 * n2);
        return nfinal / Math.sqrt(n1 * n2);
    }

    private static double getDoubleVal(Integer val) {
        val = val == null ? 0 : val;
        return Double.valueOf(val);
    }
}