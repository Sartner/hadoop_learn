package cn.sartner.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 描述:
 * 气象数据处理, 找出每年最高温度
 * 数据文件qixiang.7z
 *
 * 执行:
 * "${hadoop_home}/bin/hadoop jar hadoop_learn-1.0.jar cn.sartner.hadoop.mapreduce.MR_02_WeatherDataProcess /in/qixiang.txt /out"   文件夹都是HDFS中的
 */
public class MR_02_WeatherDataProcess {

    public static class TheMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int i=0;
            StringTokenizer itr = new StringTokenizer(value.toString());
            String year = "";
            int temp =Integer.MIN_VALUE;
            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                if(i==0){
                    year= s;
                }else if(i==4){
                    temp= Integer.parseInt(s);
                    break;
                }
                i++;
            }
            context.write(new Text(year), new IntWritable(temp));
        }
    }

    public static class TheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Integer maxTemp = Integer.MIN_VALUE;
            for (IntWritable val : values) {
                maxTemp = Math.max(maxTemp,val.get());
            }
            context.write(key, new IntWritable(maxTemp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: must have 2 parameters[<InFile> <OutFile>]");
            System.exit(2);
        }
        Job job = new Job(conf, "Weather data process");
        job.setJarByClass(MR_02_WeatherDataProcess.class);
        job.setMapperClass(TheMapper.class);
        job.setCombinerClass(TheReducer.class);
        job.setReducerClass(TheReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
