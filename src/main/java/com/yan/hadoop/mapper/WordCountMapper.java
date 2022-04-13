package com.yan.hadoop.mapper;

import com.yan.hadoop.reducer.CalculateReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text outK = new Text();
    private IntWritable outV = new IntWritable(1);
    public static Set<String> stopWords;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        getStopWords();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            if(stopWords.contains(word)){
                continue;
            }
            word = word.replaceAll("\\p{Punct}", "");
            outK.set(word);
            context.write(outK, outV);
        }
    }


    private static void getStopWords() throws IOException {
        stopWords = new HashSet<>(200);
        FileInputStream inputStream = new FileInputStream(Objects.requireNonNull(CalculateReducer.class.getClassLoader().getResource("english")).getPath());
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        List<String> lines = new ArrayList<>();
        String str = null;
        while ((str = bufferedReader.readLine()) != null) {
            str=str.replaceAll("\\p{Punct}", "");
            lines.add(str);
        }
        //close
        inputStream.close();
        bufferedReader.close();
        stopWords.addAll(lines);
    }
}
