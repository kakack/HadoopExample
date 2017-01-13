package com.kakahdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

/**
 * Created by kakack on 2017/1/12.
 */
public class FileCat {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: filecat <source>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[0]), conf);
        InputStream in = null;
        try{
            in = fs.open(new Path(args[0]));
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }

    }

}
