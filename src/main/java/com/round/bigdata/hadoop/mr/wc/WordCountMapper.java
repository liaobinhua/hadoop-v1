package com.round.bigdata.hadoop.mr.wc;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-27 14:15
 **/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Map任务读数据的key类型, offset, 是每行数据起始位置的偏移量, Long
 * VALUEIN: Map任务读数据的value类型, 其实就是一行行的字符串, String
 *
 * KEYOUT: map方法自定义实现输出的key类型, String
 * VALUEOUT: map方法自定义实现输出的value类型, Integer
 *
 * 词频统计: 相同单词次数 (world, 1)
 * Long, String, String, Integer 是Java 中的数据类型
 * Hadoop 自定义类型: 序列化和反序列化
 *
 * LongWritable, Text
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 把value 对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for(String word : words) {
            context.write(new Text(word.toLowerCase()), new IntWritable(1));
        }
    }
}
