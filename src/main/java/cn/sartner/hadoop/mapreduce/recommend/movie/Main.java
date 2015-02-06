package cn.sartner.hadoop.mapreduce.recommend.movie;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
 * 电影推荐
 */
public class Main {

    private static final Path basePath = new Path("/recommend_movie");

    public static void main(String[] args) throws Exception {

        init();

        //1. 生成电影同现矩阵
        Step1.run();
        Step2.run();
        Step3.run();
//        Step4.run();


        //2. 生成用户评分矩阵

    }


    public static void init() throws Exception {
        System.setProperty("HADOOP_USER_NAME","hduser");

        Configuration conf = new Configuration();
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");

        conf.set("mapred.jop.tracker", "hdfs://centos1:9001");
        conf.set("fs.default.name", "hdfs://centos1:9000");

        //初始化
        FileSystem fs = FileSystem.get(conf);
        if(fs.isDirectory(basePath)){
            fs.delete(basePath,true);
        }
        fs.mkdirs(basePath);
        Path dataFile = new Path(basePath,"ua.base");
        if(!fs.exists(dataFile)){
            FSDataOutputStream os = fs.create(dataFile);
            os.write(FileUtils.readFileToByteArray(new File(System.getProperty("user.dir") + File.separator + "data" + File.separator + "ua.test2")));
            os.hflush();
            os.close();
        }
    }

}