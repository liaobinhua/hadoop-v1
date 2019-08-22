package com.round.bigdata.hadoop.hdfs;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-21 16:49
 **/
public class HDFSWCApp01 {

  public static void main(String[] args) throws Exception {
    // 1.读取HDFS上的文件
    Path input = new Path("/hdfsapi/test/hello.txt");

    FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"),
        new Configuration(), "root");

    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(input, false);

    CaseIgnorWordCountMapper mapper = new CaseIgnorWordCountMapper();
    Context context = new Context();

    while (iterator.hasNext()) {
      LocatedFileStatus file = iterator.next();
      FSDataInputStream in = fileSystem.open(file.getPath());
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));

      String line = "";
      while ((line = reader.readLine()) != null) {
        // 2. 处理 词频统计
        //ToDo 将计算后的结果写到Cache
        mapper.map(line, context);
      }
      reader.close();
      in.close();
    }

    // 3. 将处理的结果存起来 使用 Map
    Map<Object, Object> contextMap = context.getCacheMap();

    //4.将结果存到HDFS
    Path output = new Path("/hdfsapi/output");
    FSDataOutputStream out = fileSystem.create(new Path(output, new Path("wordN.out")));
    Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
    for (Map.Entry<Object, Object> entry : entries) {
      out.write((entry.getKey().toString() + "\t"
              + entry.getValue().toString() + "\n").getBytes());
    }
    out.close();
    fileSystem.close();
    System.out.println("HDFS API词频统计完成!!!");
  }
}
