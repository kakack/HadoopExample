package com.kakamapreducetest;

///**
// * Created by kakack on 2016/12/29.
// */
//
//
//
//import org.apache.hadoop.io.Text;
//import java.io.IOException;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//
//
//
//
//public class HBaseMapReduceRead  {
//
//    public class MyMapper extends TableMapper<Text, Text> {
//        private Text text = new Text();
//
//        public void map(ImmutableBytesWritable row, Result value, Context context)
//                throws IOException, InterruptedException {
//            String key = new String(row.get());
//            String val = new String(value.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
//            text.set(val);
//
//            System.out.println("key: " + key + "  " + "value: " + text);
//
//            try {
//                context.write(new Text(key), text);
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static class MyDriver extends Configured implements Tool {
//
//        public int run(String[] arg0) throws Exception {
//            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.zookeeper.quorum", "namenode,datanode01,datanode02,datanode03,datanode04");
//            conf.set("user.name", "ubuntu");
//
////            conf.set("hbase.zookeeper.quorum", "192.168.130.28,192.168.130.33,192.168.130.36,192.168.130.38,192.168.129.36");
////            conf.set("hbase.zookeeper.property.clientPort", "2181");
////            conf.set("hbase.master", "192.168.130.28:60000");
//
//            Job job = new Job(conf, "HBaseMapReduceRead");
//            job.setJarByClass(HBaseMapReduceRead.class);
//            Path out = new Path("/Users/apple/Personal/workspace/MapReduceTest/out");
//            job.setOutputFormatClass(TextOutputFormat.class);
//
//            FileOutputFormat.setOutputPath(job, out);
//
//
//            job.setMapperClass(MyMapper.class);
//
//            Scan scan = new Scan();
//            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//
////            scan.setCaching(600);
////            scan.setCacheBlocks(false);
//
//            TableMapReduceUtil.initTableMapperJob(
//                    "HBaseJava",
//                    scan,
//                    MyMapper.class,
//                    Text.class,
//                    Text.class,
//                    job,
//                    true
//            );
//
//            job.waitForCompletion(true);
//
//            return 0;
//        }
//    }
//
//    public static void main(String[] args) throws IOException,
//            InterruptedException, ClassNotFoundException {
//        int mr;
//
//        try {
//            mr = ToolRunner.run(new Configuration(), new MyDriver(), args);
//            System.exit(mr);
//        }catch (Exception e ){
//            e.printStackTrace();
//        }
//    }
//}
