package com.Serializable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import java.io.*;

/**
 * Created by kakack on 2017/1/13.
 */
public class IntSer {
    public static byte[] serialize(Writable w) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(out);
        w.write(dataout);
        dataout.close();
        return out.toByteArray();
    }

    public static byte[] deserialize(Writable w, byte[] bytes) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        DataInputStream datain = new DataInputStream(in);
        w.readFields(datain);
        datain.close();
        return bytes;
    }

    public static void main(String[] args) throws Exception{
        IntWritable intw = new IntWritable(7);
        byte[] bytes = serialize(intw);
        String bytes_str = StringUtils.byteToHexString(bytes);
        System.out.println(bytes_str);

        IntWritable intw2 = new IntWritable(0);
        deserialize(intw2,bytes);
        System.out.println(intw2);
    }
}
