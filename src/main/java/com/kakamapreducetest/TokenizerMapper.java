package com.kakamapreducetest;

/**
 * Created by kakack on 2017/1/9.
 */

import java.io.IOException;
import java.util.StringTokenizer;
//将符合一定格式的字符串拆开
import org.apache.hadoop.io.IntWritable;
//一个以类表示的可序列化的整数
import org.apache.hadoop.io.Text;
//Text 类是存储字符串的可比较可序列化类
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{

    IntWritable one = new IntWritable(1);
    Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()){
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}
