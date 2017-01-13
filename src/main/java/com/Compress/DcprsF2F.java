package com.Compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by kakack on 2017/1/13.
 */
public class DcprsF2F {

    public static void main(String[] args) throws Exception{

        if (args.length != 3){
            System.err.println("Usage: CprsF2F cmps_name src target");
            System.exit(2);
        }

        Class<?> codecClass = Class.forName(args[0]);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        InputStream in = null;
        OutputStream out = null;
        FileSystem fs = FileSystem.get(URI.create(args[1]), conf);

        try{
            in = codec.createInputStream(fs.open(new Path(args[1])), codec.createDecompressor());
            out = fs.create(new Path(args[2]));
            IOUtils.copyBytes(in, out, conf);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

    }

}
