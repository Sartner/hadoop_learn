package cn.sartner.hadoop.mapreduce.recommend.movie;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *  合并
 *
 *  movie user:point
 */
public class Step4 extends Configured implements Tool {


    public static class TheMapper extends Mapper<Text, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String [] keys = key.toString().split(":");
            String [] values = value.toString().split(":");
            if(keys.length==2){
                //同现矩阵
                keyOut.set(keys[0]);
                valueOut.set("A"+keys[1] + ":" + values[0]);
                context.write(keyOut,valueOut);
            }else if(keys.length==1){
                //评分矩阵
                keyOut.set(keys[0]);
                valueOut.set("B"+values[0] + ":" + values[1]);
                context.write(keyOut,valueOut);
            }else{
                throw new RuntimeException("矩阵类型解析错误");
            }

        }
    }

    public static class TheReducer extends Reducer<Text, Text, Text, FloatWritable> {

        private Text keyOut = new Text();
        private FloatWritable valueOut = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> mapA = new HashMap<>();
            Map<String,Float> mapB = new HashMap<>();

            for (Text t : values){
                String val = t.toString();
                if(val.startsWith("A")){
                    //同现矩阵
                    String [] valArray = val.substring(1).split(":");
                    mapA.put(valArray[0], Integer.valueOf(valArray[1]));

                }else if(val.startsWith("B")){
                    //评分矩阵
                    String [] valArray = val.substring(1).split(":");
                    mapB.put(valArray[0], Float.valueOf(valArray[1]));
                }
            }

            for(Map.Entry<String,Integer> entryA : mapA.entrySet()){
                for(Map.Entry<String,Float> entryB : mapB.entrySet()){
                    keyOut.set(entryB.getKey().toString() +":"+ entryA.getKey().toString() );
                    valueOut.set(entryA.getValue() * entryB.getValue());
                    context.write(keyOut,valueOut);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        ToolRunner.run(new Configuration(), new Step4(), null);
    }


    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hduser");

        Configuration conf = getConf();

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");

        conf.set("mapred.jop.tracker", "hdfs://centos1:9001");
        conf.set("fs.default.name", "hdfs://centos1:9000");


        //初始化
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path("/recommend_movie/step4-out");
        fs.delete(outputPath, true);


        //设置JOB参数
        Job job = Job.getInstance(conf, Step4.class.getSimpleName());
        job.setJarByClass(Step4.class);

        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    conf.set("key.value.separator.in.input.line", "\t");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job,  new Path("/recommend_movie/step2-out/part-r-00000"));
        FileInputFormat.addInputPath(job,  new Path("/recommend_movie/step3-out/part-r-00000"));
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

//        if (exitCode == 0)
//            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);
        return exitCode;
    }


}