package com.MapReduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by kakack on 2017/1/16.
 */
public class NewPiEst extends Configured implements Tool {
    //临时目录，存储用
    static private final Path TMP_DIR = new Path("pitmp");
    //Log
    static final Log LOG = LogFactory.getLog(NewPiEst.class);

    //Halton 序列的类
    private static class HaltonSequence{
        // bases
        static final int[] P = {2, 3};
        // maximum number of digits allowed
        static final int[] K = {63, 40};

        private long index;
        private double[] x;
        private double[][] q;
        private int[][] d;

        HaltonSequence(long startindex){
            index = startindex;
            x = new double[K.length];
            q = new double[K.length][];
            d = new int[K.length][];
            for(int i = 0; i < K.length; i++){
                q[i] = new double[K[i]];
                d[i] = new int[K[i]];
            }

            for(int i = 0; i < K.length; i++){ long k = index;
                x[i] = 0;
                for(int j = 0; j < K[i]; j++){
                    q[i][j] = (j == 0? 1.0: q[i][j-1])/P[i]; d[i][j] = (int)(k % P[i]);
                    k = (k - d[i][j])/P[i];
                    x[i] += d[i][j] * q[i][j];
                }
            }
        }

        double[] nextPoint(){
            index++;
            for(int i = 0; i < K.length; i++){
                for(int j = 0; j < K[i]; j++){
                    d[i][j]++;
                    x[i] += q[i][j];
                    if (d[i][j] < P[i]){
                        break;
                    }
                    d[i][j] = 0;
                    x[i] -= (j == 0? 1.0: q[i][j-1]);
                }
            }
            return x;
        }
    }

    //新版 API 的 Mapper 类
    public static class PiMapper extends Mapper<LongWritable, LongWritable, LongWritable,
                LongWritable> {
        public void map(LongWritable offset, LongWritable size, Context context)
                throws IOException, InterruptedException{
            final HaltonSequence hs = new HaltonSequence(offset.get());
            long nInside = 0;
            long nOutside = 0;

            for(int i = 0; i < size.get(); i++){
                final double[] point = hs.nextPoint();
                if (point[0]*point[0] + point[1]*point[1] > 1){ nOutside++;
                }else{
                    nInside++;
                }

                context.write(new LongWritable(1), new LongWritable(nOutside));
                context.write(new LongWritable(2), new LongWritable(nInside));
            }
        }
    }

    //新版 API 的 Reducer 类
    public static class PiReducer extends
            Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
        long nInside = 0;
        long nOutside = 0;

        public void reduce(LongWritable isInside, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException{
            if (isInside.get() == 2 ){
                for (LongWritable val : values) {
                    nInside += val.get(); }
            }else{
                for (LongWritable val : values) { nOutside += val.get();
                }
            }

            LOG.info("reduce-log:" + "isInside = " + isInside.get() + ", nInside = "+ nInside + ", nOutSide = "+nOutside );
        }

        //Reducer 类在结束前执行 cleanup 函数，于是在这里将 reduce 过程计算的 nInside 和 nOutSide 写入文件。
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            Path OutDir = new Path(TMP_DIR, "out");
            Path outFile = new Path(OutDir, "reduce-out");
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Writer writer = SequenceFile.createWriter(
                    fs, conf, outFile, LongWritable.class, LongWritable.class, SequenceFile.CompressionType.NONE);
            writer.append(new LongWritable(nInside), new LongWritable(nOutside));
            writer.close();
        }
    }

    public static BigDecimal estimate(int nMaps, int nSamples, Job job)throws Exception{
        LOG.info("\n\n estimate \n\n");

        //设置 Job 的 Jar，Mapper，Reducer 等等
        job.setJarByClass(NewPiEst.class);
        job.setMapperClass(PiMapper.class);
        job.setReducerClass(PiReducer.class);
        job.setNumReduceTasks(1);

        //设置输入输出格式为序列文件格式
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //设置输出键和输出值的类型
        job.setOutputKeyClass(LongWritable.class); job.setOutputValueClass(LongWritable.class);
        job.setSpeculativeExecution(false);
        Path inDir = new Path(TMP_DIR, "in"); Path outDir = new Path(TMP_DIR, "out");

        //设置输入文件所在目录和输出结果所在目录
        FileInputFormat.addInputPath(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);

        //检查目录
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if (fs.exists(TMP_DIR)){
            throw new IOException("Tmp directory " + fs.makeQualified(TMP_DIR) + " already exists, pls remove it.");
        }

        //生成目录
        if (!fs.mkdirs(inDir)){
            throw new IOException("Cannot create input directory " + inDir);
        }

        try{
            //生成若干个序列文件，每个文件放两个整数。每个序列文件将对应一个 Map 任务
            for(int i = 0; i < nMaps; i++){
                final Path file = new Path(inDir, "part"+i);
                final LongWritable offset = new LongWritable(i*nSamples);
                final LongWritable size = new LongWritable(nSamples);
                final SequenceFile.Writer writer = SequenceFile.createWriter(
                        fs, job.getConfiguration(), file,
                        LongWritable.class, LongWritable.class, SequenceFile.CompressionType.NONE);
                writer.append(offset, size);
                writer.close();
                System.out.println("wrote input for Map #" + i);
            }

            //执行 MapReduce 任务
            System.out.println("starting mapreduce job");
            final long startTime = System.currentTimeMillis();
            boolean ret = job.waitForCompletion(true);
            final double duration = (System.currentTimeMillis() - startTime)/1000.0; System.out.println("Job finished in " + duration + " seconds.");


            //从 HDFS 将 MapReduce 的结果读取出来
            Path inFile = new Path(outDir, "reduce-out");
            LongWritable nInside = new LongWritable();
            LongWritable nOutside = new LongWritable();
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, job.getConfiguration());
            reader.next(nInside, nOutside);
            reader.close();
            LOG.info("estimate-log: " + "nInside = "+nInside.get()+", nOutSide = "+nOutside.get());

            //计算 Pi 值然后返回
            return BigDecimal.valueOf(4).multiply(
                    BigDecimal.valueOf(nInside.get())).divide(
                    BigDecimal.valueOf(nInside.get() + nOutside.get()), 20, BigDecimal.ROUND_HALF_DOWN
            );
        }finally{
            fs.delete(TMP_DIR, true);
        }
    }

    public int run(String[] args) throws Exception{
        LOG.info("\n\n run \n\n");
        if (args.length != 2){
            System.err.println("Use: NewPieEst 10 10000");
            System.exit(1);
        }

        //解析参数
        int nMaps = Integer.parseInt(args[0]);
        int nSamples = Integer.parseInt(args[1]);
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); Job job = new Job(conf, "Pi estimating job");
        System.out.println("Pi = " + estimate(nMaps, nSamples, job));
        return 0;
    }

    public static void main(String[] argv) throws Exception{
        LOG.info("\n\n main \n\n");
        System.exit(ToolRunner.run(null, new NewPiEst(), argv));
    }

}
