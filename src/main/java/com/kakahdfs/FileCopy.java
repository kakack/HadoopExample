package com.kakahdfs;

/**
 * Created by kakack on 2017/1/12.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.URI;
import java.io.InputStream;

public class FileCopy {

    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.err.println("Usage: filecopy <source> <target>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        InputStream in = new BufferedInputStream(new FileInputStream(args[0]));
        FileSystem fs = FileSystem.get(URI.create(args[1]), conf);
        OutputStream out = fs.create(new Path(args[1]));
        IOUtils.copyBytes(in, out, 4096, true);
    }

}
