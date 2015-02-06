package cn.sartner.hadoop.mapreduce;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计用户回复别人的次数
 * <p/>
 * <p/>
 * <p/>
 * $ ${hadoop_home}/bin/hadoop jar hadoop_learn-1.0.jar cn.sartner.hadoop.mapreduce.MR_10_MailReply2
 */
public class MR_10_MailReply2 extends Configured implements Tool {

    private static final Path basePath = new Path("/", MR_10_MailReply2.class.getSimpleName());
    private static final Path outputPath = new Path(basePath, "out");

    private final static Log log = LogFactory.getLog(MR_10_MailReply2.class);

    public static class TheMapper extends Mapper<Object, Text, Text, IntWritable> {

        private Text theKey = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data_array = value.toString().split("\\$\\$");
            if (data_array[1].indexOf("Re:") == -1) return;
            String sender = data_array[0].trim();

            theKey.set(sender);
            context.write(theKey, one);
        }


    }


    public static class TheReducer extends Reducer<Text, IntWritable, Text, LongWritable> {


        private LongWritable theValue = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i : values) {
                count++;
            }

            theValue.set(count);
            context.write(key, theValue);
        }

    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MR_10_MailReply2(), args);
    }


    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);

        //初始化
        fs.delete(outputPath, true);
//    fs.mkdirs(basePath);
//    initData(conf);

        //设置JOB参数
        Job job = new Job(conf, MR_10_MailReply2.class.getSimpleName());
        job.setJarByClass(MR_10_MailReply2.class);

        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);

//    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    conf.set("key.value.separator.in.input.line"," ");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job, basePath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0)
            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);

        return exitCode;
    }


}