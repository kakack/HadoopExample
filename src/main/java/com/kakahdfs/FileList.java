package com.kakahdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * Created by kakack on 2017/1/12.
 */
public class FileList {

    public static void main(String[] args) throws  Exception {
        if (args.length != 1) {
            System.err.println("Usage: filelist <source>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        FileStatus[] stat = fs.listStatus(new Path(args[0]));
        Path[] listedPaths = FileUtil.stat2Paths(stat);

        for (Path p: listedPaths){
            System.out.println(p);
        }
    }

}
