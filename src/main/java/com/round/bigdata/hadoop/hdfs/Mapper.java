package com.round.bigdata.hadoop.hdfs;

/**
 * author: binhualiao
 *
 * Created Time: 2019-08-21 22:52
 **/

public interface Mapper {

  public void map(String line, Context context);
}
