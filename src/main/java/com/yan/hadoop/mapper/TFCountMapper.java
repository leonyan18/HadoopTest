package com.yan.hadoop.mapper;

import com.yan.hadoop.client.HdfsClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class TFCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public static HdfsClient hdfsClient;
    public static String outputPath;
    public static final String PART = "/part-r-00000";
    public static final String WORD_COUNT = "Word_Count_";

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        hdfsClient = new HdfsClient(configuration.get("fs.defaultFS"), configuration);
        outputPath = configuration.get("output_path");
        try {
            hdfsClient.init();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lines = line.split(" ");
        Map<String, Integer> m = new HashMap<>(2000);
        getWordCount(m, lines[1]);
        for (String word : m.keySet()) {
            context.write(new Text(word), new IntWritable(1));
        }
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

}
