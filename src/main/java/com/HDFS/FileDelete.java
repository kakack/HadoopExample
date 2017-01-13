package com.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created by kakack on 2017/1/12.
 */
public class FileDelete {

    public static void main(String[] args) throws  Exception {
        if (args.length != 1){
            System.err.println("Usage: filedelete <target>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
        fs.delete(new Path(args[0]),false);
    }

}
