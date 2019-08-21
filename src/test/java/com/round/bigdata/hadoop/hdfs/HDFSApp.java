package com.round.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;


/**
 * author: binhualiao
 * <p>
 * Created Time: 2019-08-21 12:54
 **/
public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://hadoop000:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("--------setUp-------");
        configuration = new Configuration();
        configuration.set("dfs.replication", "1");

        /**
         * 构造访问指定HDFS系统客户端对象
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "root");
    }

    /**
     * 创建HDFS文件夹
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hadoop/test2"));
    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hadoop/test2/b.txt"));
        out.writeUTF("hello Hadoop!!!!: replication one");
        out.flush();
        out.close();
    }

    /**
     * 修改文件名
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hadoop/test2/b.txt");
        Path newPath = new Path("/hadoop/test2/a.txt");
        boolean result = fileSystem.rename(oldPath, newPath);
        System.out.println(result);
    }

    /**
     * 复制本地文件到HDFS
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path src = new Path("/Users/tal101/Project/Java/BigData/test1");
        Path path = new Path("/hadoop/test2");
        fileSystem.copyFromLocalFile(src, path);
    }

    /**
     * 复制大文件到HDFS
     *
     * @throws Exception
     */
    public void copyFromLocalBigFile() throws Exception {
        InputStream in = new BufferedInputStream(new FileInputStream(
                new File("/Users/tal101/Project/Java/BigData/coding301-master/coding301/hadoop-train-v2/ip/qqwry.dat")))
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("--------tearDown------");
    }

//    public static void main(String[] args) throws Exception {
//        Configuration configuration = new Configuration();
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "root");
//        Path src = new Path("/Users/tal101/Project/Java/BigData/test1");
//        Path path = new Path("/hadoop/test");
//        fileSystem.copyFromLocalFile(src, path);
////        boolean result = fileSystem.mkdirs(path);
////        System.out.println(result);
//    }
}
