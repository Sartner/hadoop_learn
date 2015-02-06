package cn.sartner.hadoop.mapreduce;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 自连接, 下面是一组父子对应关系, 找出第三代的对应关系(孙子 爷爷/奶奶)
 * CHILD    PARENT
 * tom      lucy
 * tom      jack
 * jone     lucy
 * jone     jack
 * lucy     mary
 * lucy     ben
 * jack     alice
 * jack     jesse
 * terry    alice
 * terry    jesse
 * philip   terry
 * philip   alma
 * mark     terry
 * mark     alma
 *
 *
 *
 *
 * $ ${hadoop_home}/bin/hadoop jar hadoop_learn-1.0.jar cn.sartner.hadoop.mapreduce.MR_08_SelfLink
 */
public class MR_08_SelfLink extends Configured implements Tool {

  private static final Path basePath = new Path(MR_08_SelfLink.class.getSimpleName());
  private static final Path outputPath = new Path(basePath,"out");

  private final static Log log = LogFactory.getLog(MR_08_SelfLink.class);

  public static class TheMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String [] values = value.toString().split(" ");
      String child = values[0].trim();
      String parent = values[1].trim();
      context.write(new Text(child), new Text(parent+"3"));
      context.write(new Text(parent),new Text(child+"1"));
    }

  }


  public static class TheReducer extends Reducer<Text, Text, Text, Text> {



    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

      List<String> children  = new ArrayList<>();
      List<String> grandParents  = new ArrayList<>();

      for(Text v : values){
        String val = v.toString();
        String name = val.substring(0,val.length()-1);
        if(val.endsWith("1")){
          children.add(name);
        }else {
          grandParents.add(name);
        }
      }
      if(children.size()>0 && grandParents.size()>0){
        for(String child : children){
          for(String grandParent : grandParents){
            context.write(new Text(child),new Text(grandParent));
          }
        }
      }

    }

  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MR_08_SelfLink(), args);
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
    Job job = new Job(conf, MR_08_SelfLink.class.getSimpleName());
    job.setJarByClass(MR_08_SelfLink.class);

    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

//    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    conf.set("key.value.separator.in.input.line"," ");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

//    job.setSortComparatorClass(TheComparator.class);


    //设置输入输入出
    FileInputFormat.addInputPath(job, basePath);
    FileOutputFormat.setOutputPath(job, outputPath);

    //执行JOB
    int exitCode = job.waitForCompletion(true) ? 0 : 1;

    if(exitCode==0)
      IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);

    return exitCode;
  }


  /**
   * 创建数据, 创建3个文本文件 每个文本文件含有5个数字
   */
  private void initData(Configuration conf) throws IOException {

    FileSystem fs = FileSystem.get(conf);

    StringBuilder result = new StringBuilder();
    result.append("tom").append(" ").append("lucy").append(System.getProperty("line.separator"));
    result.append("tom").append(" ").append("jack").append(System.getProperty("line.separator"));
    result.append("jone").append(" ").append("lucy").append(System.getProperty("line.separator"));
    result.append("jone").append(" ").append("jack").append(System.getProperty("line.separator"));
    result.append("lucy").append(" ").append("mary").append(System.getProperty("line.separator"));
    result.append("lucy").append(" ").append("ben").append(System.getProperty("line.separator"));
    result.append("jack").append(" ").append("alice").append(System.getProperty("line.separator"));
    result.append("jack").append(" ").append("jesse").append(System.getProperty("line.separator"));
    result.append("terry").append(" ").append("alice").append(System.getProperty("line.separator"));
    result.append("terry").append(" ").append("jesse").append(System.getProperty("line.separator"));
    result.append("philip").append(" ").append("terry").append(System.getProperty("line.separator"));
    result.append("philip").append(" ").append("alma").append(System.getProperty("line.separator"));
    result.append("mark").append(" ").append("terry").append(System.getProperty("line.separator"));
    result.append("mark").append(" ").append("alma").append(System.getProperty("line.separator"));

    Path file = new Path(basePath,"1.txt");
    OutputStream out=fs.create(file);
    IOUtils.copyBytes(new ByteArrayInputStream(result.toString().getBytes()), out, conf);
  }


}