package com.round.bigdata.hadoop.hdfs;

/**
 * author: binhualiao
 *
 * Created Time: 2019-08-21 22:51
 **/

public class CaseIgnorWordCountMapper implements Mapper {

  @Override
  public void map(String line, Context context) {
    String[] words = line.toLowerCase().split("\t");
    for (String word : words) {
      Object value = context.get(word);
      if (value == null) {
        context.write(word, 1);
      } else {
        int v = Integer.parseInt(value.toString());
        context.write(word, v+1);
      }
    }
  }
}
