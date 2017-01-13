package com.Compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.util.SystemClock;

import java.io.OutputStream;
import java.net.URI;

/**
 * Created by kakack on 2017/1/13.
 */
public class CprsIn2F {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CprsIn2F cmps_name target");
            System.exit(2);
        }

        Class<?> codecClass = Class.forName(args[0]);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        OutputStream out = null;
        FileSystem fs = FileSystem.get(URI.create(args[1]), conf);

        try{
            out = codec.createOutputStream(fs.create(new Path(args[1])));
            IOUtils.copyBytes(System.in, out, 4096, false);
        }finally {
            IOUtils.closeStream(out);
        }
    }
}
