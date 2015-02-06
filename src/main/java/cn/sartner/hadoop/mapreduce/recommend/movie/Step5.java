package cn.sartner.hadoop.mapreduce.recommend.movie;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 *  合并结果集, 去掉用户已经看过的电影
 */
public class Step5 extends Configured implements Tool {


    public static class TheMapper extends Mapper<Text, Text, Text, Text> {

        private boolean isUserMoviePoints = false;
        private Text keyOut = new Text();
        private Text valueOut = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            isUserMoviePoints = split.getPath().toString().contains("/step4-out/");
            super.setup(context);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            if(!isUserMoviePoints){
                String [] valueArray = value.toString().split(":");
                keyOut.set(valueArray[0]);
                valueOut.set("A"+key.toString());
                context.write(keyOut, valueOut);
            }else{
                String [] keys = key.toString().split(":");
                keyOut.set(keys[0]);
                valueOut.set("B"+keys[1]+":"+value.toString());
                context.write(keyOut, valueOut);
            }

        }
    }

    public static class TheReducer extends Reducer<Text, Text, Text, Text> {

        private Text valueOut = new Text();



        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> readMovies = new HashSet<>();
            Map<String,Float> userMovieRecommendPoints = new HashMap<>();

            for (Text t : values){
                String val = t.toString();
                if(val.startsWith("A")){
                    //用户看过的电影
                    String [] valArray = val.substring(1).split(":");
                    readMovies.add(valArray[0]);

                }else if(val.startsWith("B")){
                    //评分矩阵
                    String [] valArray = val.substring(1).split(":");
                    Float points = userMovieRecommendPoints.get(valArray[0]);
                    if(points==null){
                        points=0f;
                    }
                    userMovieRecommendPoints.put(valArray[0], points+Float.valueOf(valArray[1]));
                }
            }

            for(String m : readMovies){
                userMovieRecommendPoints.remove(m);
            }

            List<Map.Entry<String,Float>> l = new ArrayList<>(userMovieRecommendPoints.entrySet());
            Collections.sort(l, new Comparator<Map.Entry<String, Float>>() {
                @Override
                public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                    return (int) (o2.getValue()-o1.getValue());
                }
            });

            for(Map.Entry<String,Float> entry : l){
                valueOut.set(entry.getKey()+":"+entry.getValue());
                context.write(key,valueOut);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        ToolRunner.run(new Configuration(), new Step5(), null);
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
        Path outputPath = new Path("/recommend_movie/step5-out");
        fs.delete(outputPath, true);


        //设置JOB参数
        Job job = Job.getInstance(conf, Step5.class.getSimpleName());
        job.setJarByClass(Step5.class);

        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        conf.set("key.value.separator.in.input.line", "\t");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job,  new Path("/recommend_movie/step3-out/part-r-00000"));
        FileInputFormat.addInputPath(job,  new Path("/recommend_movie/step4-out/part-r-00000"));
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

//        if (exitCode == 0)
//            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);
        return exitCode;
    }


}