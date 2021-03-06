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


/*
* 需要压缩的情况:1,在HDFS上存文件;2,在集群间通信需要压缩
*
* 做压缩:有输入流A和输出流B，选择压缩算法,创建压缩器,
* 然后用压缩器和输出流B创建压缩输出流C,
* 最后将数据从输入流A复制到压缩输出流C即可进行压缩并输出结果。
*
* 解压缩:选择解压缩算法,创建相应压缩器,用压缩器和输入流A创建压缩输入流C,
* 最后数据从输入流C复制到输出流B即可进行解压缩并输出结果
*
*
* */

public class CprsF2F {
    public static void main(String[] args) throws Exception{
        if(args.length != 3){
            System.err.println("Usage: CprsF2F cmps_name src target");
            System.exit(2);
        }

        Class<?> codecClass = Class.forName(args[0]);
        Configuration conf = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        InputStream in = null;
        OutputStream out = null;
        FileSystem fs = FileSystem.get(URI.create(args[1]),conf);

        try{
            in = fs.open(new Path(args[1]));
            out = codec.createOutputStream(fs.create(new Path(args[2])));
            IOUtils.copyBytes(in, out, conf);
        }finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }

}
