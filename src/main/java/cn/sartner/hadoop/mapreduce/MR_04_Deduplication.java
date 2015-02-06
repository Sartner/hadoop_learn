package cn.sartner.hadoop.mapreduce;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * 数据去重
 * 文件1: a b c d
 * 文件2: d c a
 * 文件3: f c
 * 文件4: a z
 *
 * 得到结果 a b c d f z
 *
 */
public class MR_04_Deduplication extends Configured implements Tool {

  public static class TheMapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      context.write(value,NullWritable.get());
    }

  }


  public static class TheReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key,NullWritable.get());
    }

  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MR_04_Deduplication(), args);
  }



  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    FileSystem fs = FileSystem.get(conf);
    FileSystem.get(conf).delete(new Path("/MR04"),true);


    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "Deduplication");
    job.setJarByClass(MR_04_Deduplication.class);

    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    Path inputPath = initData(conf);
    FileInputFormat.addInputPath(job, inputPath);

    Path outputPath = new Path("/MR04/out");
    FileOutputFormat.setOutputPath(job, outputPath);

    int result = job.waitForCompletion(true) ? 0 : 1;


    //输出
    IOUtils.copyBytes(fs.open(new Path("/MR04/out", "part-r-00000")), System.out, 4096, false);

    return result;
  }

  /**
   * 创建数据
   */
  private Path initData(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    String l = System.getProperty("line.separator");
    IOUtils.copyBytes(new ByteArrayInputStream(new StringBuilder("a").append(l).append("b").append(l).append("c").append(l).append("d").toString().getBytes()), fs.create(new Path("/MR04/in/1.txt")), conf);
    IOUtils.copyBytes(new ByteArrayInputStream(new StringBuilder("d").append(l).append("c").append(l).append("a").toString().getBytes()), fs.create(new Path("/MR04/in/2.txt")), conf);
    IOUtils.copyBytes(new ByteArrayInputStream(new StringBuilder("f").append(l).append("c").toString().getBytes()), fs.create(new Path("/MR04/in/3.txt")), conf);
    IOUtils.copyBytes(new ByteArrayInputStream(new StringBuilder("a").append(l).append("z").toString().getBytes()), fs.create(new Path("/MR04/in/4.txt")), conf);
    return new Path("/MR04/in");
  }



}