package cn.sartner.hadoop.mapreduce;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sun.nio.ch.IOUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * 排序并输出序号
 *
 *
 * $ ${hadoop_home}/bin/hadoop jar hadoop_learn-1.0.jar cn.sartner.hadoop.mapreduce.MR_06_Order2
 */
public class MR_06_Order2 extends Configured implements Tool {

  private static final Path basePath = new Path(MR_06_Order2.class.getSimpleName());
  private static final Path outputPath = new Path(basePath,"out");

  private final static Log log = LogFactory.getLog(MR_06_Order2.class);

  enum Error{
    NUM_FMT
  }

  public static class TheMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    private static final IntWritable one = new IntWritable(1);


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
        context.write(new IntWritable(Integer.parseInt(value.toString())),one);
      }catch (NumberFormatException e) {
        Counter c = context.getCounter(Error.NUM_FMT);
        c.increment(1);
      }
    }

  }


  public static class TheReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private int rownum=1;

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      for(IntWritable i : values){
        context.write(new IntWritable(rownum++),key);
      }

    }

  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MR_06_Order2(), args);
  }


  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);

    //初始化
    fs.delete(basePath, true);
    fs.mkdirs(basePath);
    initData(conf);

    //设置JOB参数
    Job job = new Job(conf, MR_06_Order2.class.getSimpleName());
    job.setJarByClass(MR_06_Order2.class);

    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setInputFormatClass(TextInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);



    //设置输入输入出
    FileInputFormat.addInputPath(job, basePath);
    FileOutputFormat.setOutputPath(job, outputPath);

    //执行JOB
    int result = job.waitForCompletion(true) ? 0 : 1;

    //打印输出
    Counters counters = job.getCounters();
    Counter counter1=counters.findCounter(Error.NUM_FMT);
    log.info("number format errors:" + counter1.getValue());

    if(result==0)
      IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);

    return result;
  }


  /**
   * 创建数据, 创建3个文本文件 每个文本文件含有5个数字
   */
  private void initData(Configuration conf) throws IOException {

    Random r = new Random();
    FileSystem fs = FileSystem.get(conf);

    for (int i = 0; i < 3; i++) {
      StringBuilder result = new StringBuilder();

      for (int i2 = 0; i2 < 5; i2++) {
        result.append(r.nextInt(100)).append(System.getProperty("line.separator"));
      }


      Path file = new Path(basePath,i+".txt");
      OutputStream out=fs.create(file);
      IOUtils.copyBytes(new ByteArrayInputStream(result.toString().getBytes()), out, conf);
    }
  }



}