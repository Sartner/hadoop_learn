package cn.sartner.hadoop.mapreduce;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统计用户被别人回复的次数(排除自己回复的)
 *
 *
 *
 * $ ${hadoop_home}/bin/hadoop jar hadoop_learn-1.0.jar cn.sartner.hadoop.mapreduce.MR_09_MailReply
 */
  public class MR_09_MailReply extends Configured implements Tool {

  private static final Path basePath = new Path("/",MR_09_MailReply.class.getSimpleName());
  private static final Path outputPath = new Path(basePath,"out");

  private final static Log log = LogFactory.getLog(MR_09_MailReply.class);

  public static class TheMapper extends Mapper<Object, Text, Text, Text> {

    private Text theKey = new Text();
    private Text theValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String [] data_array = value.toString().split("\\$\\$");
      String sender = data_array[0].trim();
      String topic = data_array[1].replaceAll("Re:","").trim();
      String date = data_array[2].trim();

      theKey.set(topic);
      theValue.set(sender+"#"+date);
      context.write(theKey,theValue);
    }

  }


  public static class TheReducer extends Reducer<Text, Text, Text, LongWritable> {


    private Text theKey = new Text();
    private LongWritable theValue = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      String owner = null;
      long minDate = Long.MAX_VALUE;

      Map<String,AtomicLong> sender_sendCount = new HashMap<>();

      for(Text senderAndDate : values){
        String [] senderAndDate_array = senderAndDate.toString().split("#");
        String sender =senderAndDate_array[0];
        long sendDate = Long.parseLong(senderAndDate_array[1]);
        if(sendDate<minDate){
          minDate=sendDate;
          owner=sender;
        }

        AtomicLong atomicLong =  sender_sendCount.get(sender);
        if(null==atomicLong){
          atomicLong=new AtomicLong(0);
          sender_sendCount.put(sender,atomicLong);
        }
        atomicLong.addAndGet(1);
      }

      sender_sendCount.remove(owner);

      long count = 0;
      for(AtomicLong atomicLong : sender_sendCount.values()){
        count+=atomicLong.longValue();
      }

      theKey.set(owner);
      theValue.set(count);
      context.write(theKey,theValue);
    }

  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MR_09_MailReply(), args);
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
    Job job = new Job(conf, MR_09_MailReply.class.getSimpleName());
    job.setJarByClass(MR_09_MailReply.class);

    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

//    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    conf.set("key.value.separator.in.input.line"," ");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    //设置输入输入出
    FileInputFormat.addInputPath(job, basePath);
    FileOutputFormat.setOutputPath(job, outputPath);

    //执行JOB
    int exitCode = job.waitForCompletion(true) ? 0 : 1;

    if(exitCode==0)
      IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);

    return exitCode;
  }


}