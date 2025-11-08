package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    public static void main(String[] args) {


    }

    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        System.out.println("test method executed");
        Configuration conf = new Configuration();
   /*     conf.set("fs.defaultFS", "hdfs://mycluster");
        conf.set("dfs.nameservices", "mycluster");
        conf.set("dfs.ha.namenodes.mycluster", "nn1,nn2,nn3");
        conf.set("dfs.namenode.rpc-address.mycluster.nn1", "hadoop101:8020");
        conf.set("dfs.namenode.rpc-address.mycluster.nn2", "hadoop102:8020");
        conf.set("dfs.namenode.rpc-address.mycluster.nn3", "hadoop103:8020");
        conf.set("dfs.client.failover.proxy.provider.mycluster",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");*/

        FileSystem fs = FileSystem.get(new URI("hdfs://mycluster"), conf, "atguigu");

        fs.mkdirs(new Path("/aaaa/bbbb"));

        fs.close();

    }

    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set("dfs.replication", "3");
        FileSystem fs = FileSystem.get(new URI("hdfs://mycluster"), conf, "atguigu");

        fs.copyFromLocalFile(new Path("/Users/richard/Documents/learningdoc/面试题/test.txt"),
                new Path("/test.txt"));

        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://mycluster"), conf, "atguigu");

        fs.copyToLocalFile(new Path("/尚硅谷大数据技术之高频面试题9.1.3.docx"),
                new Path("/Users/richard/Documents/learningdoc/面试题/尚硅谷大数据技术之高频面试题9.1.3_copy.docx"));

        fs.close();
    }
}
