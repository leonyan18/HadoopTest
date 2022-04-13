package com.yan.hadoop.driver;

import com.yan.hadoop.client.HdfsClient;
import com.yan.hadoop.combiner.WordCountCombiner;
import com.yan.hadoop.mapper.CalculateMapper;
import com.yan.hadoop.mapper.TFCountMapper;
import com.yan.hadoop.mapper.WordCountMapper;
import com.yan.hadoop.reducer.CalculateReducer;
import com.yan.hadoop.reducer.TFCountReducer;
import com.yan.hadoop.reducer.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class WordCountDriver {
    public static final String WORD_COUNT_PRE = "Word_Count";
    public static final String CALCULATE = "Calculate";
    public static final String TF_COUNT_PRE = "TF_Count";
    public static final String SP = "_";
    public static final String BASE_URL = "your";
    public static Configuration conf;
    public static HdfsClient client;

    public static void init() throws URISyntaxException, IOException, InterruptedException {
        conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("fs.defaultFS", BASE_URL);
        conf.set("mapreduce.output.basename" ,"part");
        client = new HdfsClient(BASE_URL, conf);
        client.init();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // 1 获取job
        init();
        String input = "/yan/input/";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        String date = sdf.format(new Date());
        String output = "/yan/output/" + date + "/";
        conf.set("output_path", output);
        conf.set("input_path", input);
        String listUrl = "list.txt";
        List<String> files = Arrays.asList(client.readFileContext(input + listUrl).split("\n"));
        conf.set("document_size", String.valueOf(files.size()));
        conf.set("word_count_path", output);
        doWorkJobs(files, input, output);
        doTFCount(input + listUrl, output);
        doCalculate(input + listUrl, output);
        client.close();
    }

    public static void doWorkJobs(List<String> files, String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        List<Job> jobs = new ArrayList<>();
        for (String f : files) {
            f = f.split(" ")[1];
            jobs.add(doWordCount(input + f, output + WORD_COUNT_PRE + SP + f));
        }
        for (Job j : jobs) {
            j.waitForCompletion(true);
        }
    }

    public static Job doWordCount(String input, String output) throws IOException {
        Job job = Job.getInstance(conf, WORD_COUNT_PRE);
        // 2 设置jar包路径
        job.setJarByClass(WordCountDriver.class);
        // 3 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setCombinerClass(WordCountCombiner.class);
//        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(BASE_URL + input));
        FileOutputFormat.setOutputPath(job, new Path(BASE_URL + output));
        return job;
    }

    public static void doCalculate(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, CALCULATE);
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(CalculateMapper.class);
        job.setReducerClass(CalculateReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(BASE_URL + input));
        FileOutputFormat.setOutputPath(job, new Path(BASE_URL + output + "result/"));
        job.waitForCompletion(true);
    }

    public static void doTFCount(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, TF_COUNT_PRE);
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(TFCountMapper.class);
        job.setReducerClass(TFCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(BASE_URL + input));
        FileOutputFormat.setOutputPath(job, new Path(BASE_URL + output + "TF_Count/"));
        job.waitForCompletion(true);
    }
}
