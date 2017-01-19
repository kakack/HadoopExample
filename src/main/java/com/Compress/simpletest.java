package com.Compress;

import org.apache.hadoop.io.Writable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by kakack on 2017/1/18.
 */
public class simpletest {

    public static byte[] serialize(Writable writable) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataout = new DataOutputStream(out);
        writable.write(dataout);
        dataout.close();
        return out.toByteArray();
    }

}
