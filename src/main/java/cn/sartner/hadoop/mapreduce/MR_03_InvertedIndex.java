package cn.sartner.hadoop.mapreduce;


import com.google.common.base.Charsets;
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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * 倒排索引
 * 现有一批电话通话清单, 记录了用户A拨打给用户B的记录
 * 需要做一个倒排索引, 记录拨打给用户B的所有用户A
 *
 *
 */
public class MR_03_InvertedIndex extends Configured implements Tool {

  public static class TheMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
      context.write(value,key);
    }

  }


  public static class TheReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      StringBuffer result = new StringBuffer();
      for(Text v : values){
        result.append(v.toString()).append(",");
      }
      context.write(key,new Text(result.substring(0,result.length()-1)));
    }

  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MR_03_InvertedIndex(), args);
  }


  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: <out>");
      return 1;
    }


    Configuration conf = getConf();
    conf.set("key.value.separator.in.input.line"," ");



    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "Inversed Index");
    job.setJarByClass(MR_03_InvertedIndex.class);
    job.setMapperClass(TheMapper.class);
    job.setCombinerClass(TheReducer.class);
    job.setReducerClass(TheReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);


    FileInputFormat.addInputPath(job, initData(conf));
    Path outputpath = new Path(args[0]);
    FileSystem.get(conf).delete(outputpath,true);
    FileOutputFormat.setOutputPath(job, outputpath);

    return (job.waitForCompletion(true) ? 0 : 1);
  }

  /**
   * 创建数据
   */
  private Path initData(Configuration conf) throws IOException {
    String [] s1 = new String[]{"13666666666","13777777777","13888888888"}; //主叫号码
    String [] s2 = new String[]{"13800138000","120","10086","10010","10000","110","119"}; //被叫号码

    StringBuilder result = new StringBuilder();
    Random r = new Random();
    for (int i = 0; i < 10; i++) {
      result.append(s1[r.nextInt(s1.length)]).append(" ").append(s2[r.nextInt(s2.length)]).append(System.getProperty("line.separator"));
    }


    FileSystem fs = FileSystem.get(conf);
    String inputFile = String.valueOf(System.currentTimeMillis())+".txt";
    Path file = new Path(inputFile);
    OutputStream out=fs.create(file);
    IOUtils.copyBytes(new ByteArrayInputStream(result.toString().getBytes()), out, conf);
    return file;
  }
}