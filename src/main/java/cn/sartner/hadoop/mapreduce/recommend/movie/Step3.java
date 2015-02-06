package cn.sartner.hadoop.mapreduce.recommend.movie;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.function.Consumer;

/**
 *  生成电影评分矩阵
 *
 *  movie user:point
 */
public class Step3 extends Configured implements Tool {


    public static class TheMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String [] values = value.toString().split("\t");
            keyOut.set(values[1]);
            valueOut.set(values[0]+":"+values[2]);
            context.write(keyOut, valueOut);
        }
    }

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        ToolRunner.run(new Configuration(), new Step3(), null);
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
        Path outputPath = new Path("/recommend_movie/step3-out");
        fs.delete(outputPath, true);


        //设置JOB参数
        Job job = Job.getInstance(conf, Step3.class.getSimpleName());
        job.setJarByClass(Step3.class);

        job.setMapperClass(TheMapper.class);
//        job.setReducerClass(TheReducer.class);

//    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    conf.set("key.value.separator.in.input.line"," ");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job,  new Path("/recommend_movie/ua.base"));
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

//        if (exitCode == 0)
//            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);
        return exitCode;
    }


}