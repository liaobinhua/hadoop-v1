package com.round.bigdata.hadoop.mr.wc;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-27 14:15
 **/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * (world, 1)
     * map的输出到reduce端, 是按照相同的key 分发到一个reduce上去执行
     *
     * reduce1: (world, 1) => (world, <1>)
     * reduce2: (hello, 1)(hello, 1) => (hello, <1, 1>)
     * Mapper 和 Reduce 使用的设计模式: 模板
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()) {
            IntWritable value = iterator.next();
            count += value.get();
        }

        context.write(key, new IntWritable(count));
    }

}
