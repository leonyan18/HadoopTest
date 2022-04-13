package com.yan.hadoop.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yan
 */
public class CalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Integer size = 0;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        size = Integer.valueOf(conf.get("document_size"));
    }

    @Override
    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        String[] temp = ivalue.toString().split(" ");
        int id = Integer.parseInt(temp[0]);
        for (int i = 1; i < id; i++) {
            String id1 = i + "," + id;
            context.write(new Text(id1), new Text(temp[1]));
        }
        for (int i = id + 1; i <= size; i++) {
            String id1 = id + "," + i;
            context.write(new Text(id1), new Text(temp[1]));
        }
    }

}
