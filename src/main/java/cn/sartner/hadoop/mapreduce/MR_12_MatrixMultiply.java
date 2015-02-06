package cn.sartner.hadoop.mapreduce;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * hadoop矩阵计算, 需知矩阵大小
 */
public class MR_12_MatrixMultiply extends Configured implements Tool {

    private static final Path basePath = new Path("/",MR_12_MatrixMultiply.class.getSimpleName());
    private static final Path outputPath = new Path(basePath,"out");
    private static final Path inputPath = new Path(basePath,"in");

    /*
    * 矩阵大小需已知, 矩阵A*矩阵B,得到矩阵C的大小为2*3
    * */
    private static final int matrixA_lines = 2;
    private static final int matrixA_columns = 5;
    private static final int matrixB_lines = 5;
    private static final int matrixB_columns = 3;


    /**
     * 输出
     * 1,1 A,0,0
     * 1,1 A,1,1
     * 1,1 A,2,2
     */
    public static class TheMapper extends Mapper<LongWritable, Text, Text, Text>
    {

        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String [] values = value.toString().split(",");
            int pos_x = Integer.parseInt(values[1]);
            int pos_y = Integer.parseInt(values[2]);
            double pos_value = Double.parseDouble(values[3]);

            if(values[0].equals("A")){
                //matrix A
                for (int i = 0; i < matrixB_columns; i++) {
                    context.write(new Text(pos_x+","+i),new Text("A,"+pos_y+","+pos_value));
                }
            }else{
                //matrix B
                for (int i = 0; i < matrixA_lines; i++) {
                    context.write(new Text(i+","+pos_y),new Text("B,"+pos_x+","+pos_value));
                }
            }
        }
    }

    /**
     * 统计同一个IMSI在同一时间段
     * 在不同CGI停留的时长
     */
    public static class TheReducer extends Reducer<Text, Text, Text, DoubleWritable>
    {

        public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
        {

            Map<String,MatrixValue> map = new HashMap<>();

            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()){
                String theV = iterator.next().toString();
                String [] v = theV.split(",");
                String pos = v[1];
                Double pos_value = Double.valueOf(v[2]);
                MatrixValue d = map.get(pos);
                if(d==null){
                    d=new MatrixValue();
                    map.put(pos,d);
                }
                d.addValue(pos_value);
            }
            //稀疏矩阵处理
            double result =0;
            for(MatrixValue v : map.values()){
                result+=v.getValue();
            }
            context.write(key,new DoubleWritable(result));
        }

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MR_12_MatrixMultiply(), args);
    }


    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hduser");

        Configuration conf = getConf();


        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");

        conf.set("mapred.jop.tracker", "hdfs://centos1:9001");
        conf.set("fs.default.name", "hdfs://centos1:9000");

        //初始化
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);
        fs.mkdirs(inputPath);
//    initData(conf);


        Path dataFile1 = new Path(inputPath,"matrix.csv");
        if(!fs.exists(dataFile1)){
            FSDataOutputStream os = fs.create(dataFile1);
            os.write(FileUtils.readFileToByteArray(new File(System.getProperty("user.dir") + File.separator + "data" + File.separator +  "matrix.csv")));
            os.hflush();
            os.close();
        }


        //设置JOB参数
        Job job = Job.getInstance(conf, MR_12_MatrixMultiply.class.getSimpleName());
        job.setJarByClass(MR_12_MatrixMultiply.class);

        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);

//    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    conf.set("key.value.separator.in.input.line"," ");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0)
            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);
        return exitCode;
    }

    private static class MatrixValue{
        private double [] pos = new double[2];

        public void addValue(double value){
            if(pos[0]==0){
                pos[0]=value;
            }else {
                pos[1]=value;
            }
        }

        public double getValue(){
            return pos[0]*pos[1];
        }


    }

}