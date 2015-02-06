package cn.sartner.hadoop.mapreduce.recommend.movie;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 *  通过第一步输出, 生成物品同现矩阵
 *
 *  movie:movie count
 */
public class Step2 extends Configured implements Tool {


    public static class TheMapper extends Mapper<Text, Text, Text, IntWritable> {

        private Text keyOut = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Text user, Text value, Context context) throws IOException, InterruptedException {
            String [] values = value.toString().split(",");
            List<String> movies = new ArrayList<>(values.length);
            for (int i = 0; i < values.length; i++) {
               String [] movie_point_array = values[i].split(":");
                movies.add(movie_point_array[0]);
            }

            for(String movie1 : movies){
                for(String movie2 : movies){
                    keyOut.set(movie1+":"+movie2);
                    context.write(keyOut,one);
                }
            }
        }
    }

    public static class TheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable value = new IntWritable(1);

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int i =0;
            while (iterator.hasNext()){
                iterator.next();
                i++;
            }
            value.set(i);
            context.write(key,value);
        }
    }

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        ToolRunner.run(new Configuration(), new Step2(), null);
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
        Path outputPath = new Path("/recommend_movie/step2-out");
        fs.delete(outputPath, true);


        //设置JOB参数
        Job job = Job.getInstance(conf, Step2.class.getSimpleName());
        job.setJarByClass(Step2.class);

        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        conf.set("key.value.separator.in.input.line","\t");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job,  new Path("/recommend_movie/step1-out/part-r-00000"));
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

//        if (exitCode == 0)
//            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);
        return exitCode;
    }


}