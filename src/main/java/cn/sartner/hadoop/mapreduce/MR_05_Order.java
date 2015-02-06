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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * 排序
 *
 *
 */
public class MR_05_Order extends Configured implements Tool {

  private final static Log log = LogFactory.getLog(MR_05_Order.class);

  enum Error{
    NUM_FMT
  }

  public static class TheMapper extends Mapper<Object, Text, IntWritable, NullWritable> {

    private IntWritable i = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
        i.set(Integer.parseInt(value.toString()));
        context.write(i,NullWritable.get());
      }catch (NumberFormatException e) {
        Counter c = context.getCounter(Error.NUM_FMT);
        c.increment(1);
      }
    }

  }


  public static class TheReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key,NullWritable.get());
    }

  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MR_05_Order(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "Order");
    job.setJarByClass(MR_05_Order.class);

    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setInputFormatClass(TextInputFormat.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(NullWritable.class);


    FileInputFormat.addInputPath(job, initData(conf));
    Path outputpath = new Path("out05");
    FileSystem.get(conf).delete(outputpath, true);
    FileOutputFormat.setOutputPath(job, outputpath);

    int result = job.waitForCompletion(true) ? 0 : 1;

    Counters counters = job.getCounters();
    Counter counter1=counters.findCounter(Error.NUM_FMT);
    log.info("number format errors:" + counter1.getValue());
    return result;
  }

  /**
   * 创建数据
   */
  private Path initData(Configuration conf) throws IOException {
    StringBuilder result = new StringBuilder();
    Random r = new Random();
    for (int i = 0; i < 10; i++) {
      result.append(r.nextInt(100)).append(System.getProperty("line.separator"));
    }


    FileSystem fs = FileSystem.get(conf);
    String inputFile = String.valueOf(System.currentTimeMillis())+".txt";
    Path file = new Path(inputFile);
    OutputStream out=fs.create(file);
    IOUtils.copyBytes(new ByteArrayInputStream(result.toString().getBytes()), out, conf);
    return file;
  }



}