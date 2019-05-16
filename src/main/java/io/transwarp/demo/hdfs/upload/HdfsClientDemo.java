package io.transwarp.demo.hdfs.upload;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.URI;

public class HdfsClientDemo {
    public static void main(String[] args) throws IOException {
        System.setProperty("java.security.krb5.conf", HdfsClientDemo.class.getClassLoader().getResource("krb5.conf").getPath());

        Configuration conf = new Configuration();

        conf.addResource(HdfsClientDemo.class.getClassLoader().getResource("core-site.xml"));
        conf.addResource(HdfsClientDemo.class.getClassLoader().getResource("hdfs-site.xml"));

        UserGroupInformation.setConfiguration(conf);
        try {
            UserGroupInformation.loginUserFromKeytab("hdfs@TDH", HdfsClientDemo.class.getClassLoader().getResource("keytab").getPath());
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        DistributedFileSystem dfs = new DistributedFileSystem();
        try {
            dfs.initialize(new URI(conf.get("fs.defaultFS")), conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

//    dfs.delete(new Path("/tmp/test.txt"));
//     dfs.createNewFile(new Path("/tmp/test.txt"));
        dfs.mkdirs(new Path("/tmp/test7"));

    }
}

