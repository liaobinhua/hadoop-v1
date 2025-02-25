package com.round.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-27 16:36
 **/
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");

        String phone = lines[1];
        long up = Long.parseLong(lines[lines.length - 3]);
        long down = Long.parseLong(lines[lines.length - 2]);
        context.write(new Text(phone), new Access(phone, up, down));
    }
}
