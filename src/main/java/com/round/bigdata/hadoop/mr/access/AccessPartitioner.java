package com.round.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-28 14:00
 **/
public class AccessPartitioner extends Partitioner<Text, Access> {
    /**
     * 自定义分区
     * @param phone
     * @param access
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(Text phone, Access access, int numPartitions) {
        if (phone.toString().startsWith("13")) {
            return 0;
        } else if (phone.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
