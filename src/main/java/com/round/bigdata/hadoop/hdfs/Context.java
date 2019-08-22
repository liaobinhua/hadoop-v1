package com.round.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * author: binhualiao
 *
 * Created Time: 2019-08-21 22:50
 **/

public class Context {

  private Map<Object, Object> cacheMap = new HashMap<Object, Object>();

  public Map<Object, Object> getCacheMap() {
    return cacheMap;
  }

  /**
   * 将数据写到缓存中
   * @param key
   * @param value
   */
  public void write(Object key, Object value) {
    cacheMap.put(key, value);
  }

  /**
   * 从缓存中获取数据
   * @param key
   * @return
   */
  public Object get(Object key) {
    return cacheMap.get(key);
  }

}
