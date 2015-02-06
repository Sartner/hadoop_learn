package cn.sartner.hadoop.mapreduce;


import cn.sartner.hadoop.mapreduce.kpi.KPI;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 基站数据处理
 */
public class MR_11_BaseStation extends Configured implements Tool {

    private static final Path basePath = new Path("/",MR_11_BaseStation.class.getSimpleName());
    private static final Path outputPath = new Path(basePath,"out");


    /**
     * 计数器
     * 用于计数各种异常数据
     */
    enum Counter
    {
        TIMESKIP,		//时间格式有误
        OUTOFTIMESKIP,	//时间不在参数指定的时间段内
        LINESKIP,		//源文件行有误
        USERSKIP		//某个用户某个时间段被整个放弃
    }

    /**
     * 读取一行数据
     * 以“IMSI+时间段”作为 KEY 发射出去
     */
    public static class TheMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        String date;
        String [] timepoint;
        boolean dataSource;

        /**
         * 初始化
         */
        public void setup ( Context context ) throws IOException
        {
            this.date = context.getConfiguration().get("date");							//读取日期
            this.timepoint = context.getConfiguration().get("timepoint").split("-");	//读取时间分割点

            //提取文件名
            FileSplit fs = (FileSplit)context.getInputSplit();
            String fileName = fs.getPath().getName();
            if( fileName.startsWith("pos") )
                dataSource = true;
            else if ( fileName.startsWith("net") )
                dataSource = false;
            else
                throw new IOException("File Name should starts with POS or NET");
        }

        /**
         * MAP任务
         * 读取基站数据
         * 找出数据所对应时间段
         * 以IMSI和时间段作为 KEY
         * CGI和时间作为 VALUE
         */
        public void map ( LongWritable key, Text value, Context context ) throws IOException, InterruptedException
        {
            String line = value.toString();
            TableLine tableLine = new TableLine();

            //读取行
            try
            {
                tableLine.set(line, this.dataSource, this.date, this.timepoint );
            }
            catch ( LineException e )
            {
                if(e.getFlag()==-1)
                    context.getCounter(Counter.OUTOFTIMESKIP).increment(1);
                else
                    context.getCounter(Counter.TIMESKIP).increment(1);
                return;
            }
            catch (Exception e)
            {
                context.getCounter(Counter.LINESKIP).increment(1);
                return;
            }

            context.write( tableLine.outKey(), tableLine.outValue() );
        }
    }

    /**
     * 统计同一个IMSI在同一时间段
     * 在不同CGI停留的时长
     */
    public static class TheReducer extends Reducer<Text, Text, NullWritable, Text>
    {
        private String date;
        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * 初始化
         */
        public void setup ( Context context )
        {
            this.date = context.getConfiguration().get("date");							//读取日期
        }

        public void reduce ( Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException
        {
            String imsi = key.toString().split("\\|")[0];
            String timeFlag = key.toString().split("\\|")[1];

            //用一个TreeMap记录时间
            TreeMap<Long, String> uploads = new TreeMap<Long, String>();
            String valueString;

            for ( Text value : values )
            {
                valueString = value.toString();
                try
                {
                    uploads.put( Long.valueOf( valueString.split("\\|")[1] ), valueString.split("\\|")[0] );
                }
                catch ( NumberFormatException e )
                {
                    context.getCounter(Counter.TIMESKIP).increment(1);
                    continue;
                }
            }

            try
            {
                //在最后添加“OFF”位置
                Date tmp = this.formatter.parse( this.date + " " + timeFlag.split("-")[1] + ":00:00" );
                uploads.put ( ( tmp.getTime() / 1000L ), "OFF");

                //汇总数据
                SortedSet<Map.Entry<String, Float>> locs = getStayTime(uploads);

                int i=0;
                //输出
                for( java.util.Map.Entry<String, Float> entry : locs )
                {

                    StringBuilder builder = new StringBuilder();
                    builder.append(imsi).append("|");
                    builder.append(entry.getKey()).append("|");
                    builder.append(timeFlag).append("|");
                    builder.append(entry.getValue());

                    context.write( NullWritable.get(), new Text(builder.toString()) );
                    if(++i==3)break;
                }
            }
            catch ( Exception e )
            {
                context.getCounter(Counter.USERSKIP).increment(1);
                return;
            }
        }

        /**
         * 获得位置停留信息
         */
        private SortedSet<Map.Entry<String, Float>> getStayTime(TreeMap<Long, String> uploads)
        {
            java.util.Map.Entry<Long, String> upload, nextUpload;
            HashMap<String, Float> locs = new HashMap<String, Float>();
            //初始化
            Iterator<java.util.Map.Entry<Long, String>> it = uploads.entrySet().iterator();
            upload = it.next();
            //计算
            while( it.hasNext() )
            {
                nextUpload = it.next();
                float diff = (float) (nextUpload.getKey()-upload.getKey()) / 60.0f;
                if( diff <= 60.0 )									//时间间隔过大则代表关机
                {
                    if( locs.containsKey( upload.getValue() ) )
                        locs.put( upload.getValue(), locs.get(upload.getValue())+diff );
                    else
                        locs.put( upload.getValue(), diff );
                }
                upload = nextUpload;
            }
            return entriesSortedByValues(locs);
        }

        static <K,V extends Comparable<? super V>>
        SortedSet<Map.Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
            SortedSet<Map.Entry<K,V>> sortedEntries = new TreeSet<Map.Entry<K,V>>(
                    new Comparator<Map.Entry<K,V>>() {
                        @Override public int compare(Map.Entry<K,V> e1, Map.Entry<K,V> e2) {
                            int res = e2.getValue().compareTo(e1.getValue());
                            return res != 0 ? res : 1;
                        }
                    }
            );
            sortedEntries.addAll(map.entrySet());
            return sortedEntries;
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new MR_11_BaseStation(), args);
    }


    @Override
    public int run(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","hduser");

        Configuration conf = getConf();


        conf.set("date", "2015-01-01");
        conf.set("timepoint", "9-17");

        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/yarn-site.xml");

        conf.set("mapred.jop.tracker", "hdfs://centos1:9001");
        conf.set("fs.default.name", "hdfs://centos1:9000");

        //初始化
        FileSystem fs = FileSystem.get(conf);
        fs.delete(outputPath, true);
//    fs.mkdirs(basePath);
//    initData(conf);

        //设置JOB参数
        Job job = Job.getInstance(conf, MR_11_BaseStation.class.getSimpleName());
        job.setJarByClass(MR_11_BaseStation.class);

        job.setMapperClass(TheMapper.class);
        job.setReducerClass(TheReducer.class);

//    job.setInputFormatClass(KeyValueTextInputFormat.class);
//    conf.set("key.value.separator.in.input.line"," ");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //设置输入输入出
        FileInputFormat.addInputPath(job, basePath);
        FileOutputFormat.setOutputPath(job, outputPath);

        //执行JOB
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        if (exitCode == 0)
            IOUtils.copyBytes(fs.open(new Path(outputPath, "part-r-00000")), System.out, 4096, false);
        return exitCode;
    }


    /**
     * 定义异常类
     */
    private static class LineException extends Exception
    {
        private static final long serialVersionUID = 8245008693589452584L;
        int flag;
        public LineException(String msg, int flag)
        {
            super(msg);
            this.flag = flag;
        }
        public int getFlag()
        {
            return flag;
        }
    }


    /**
     * 读取一行数据
     * 提取所要字段
     */
    private static class TableLine
    {
        private String imsi, position, time, timeFlag;
        private Date day;
        private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        /**
         * 初始化并检查该行的合法性
         */
        public void set ( String line, boolean source, String date, String [] timepoint ) throws LineException
        {
            String [] lineSplit = line.split("\t");
            if( source )
            {
                this.imsi = lineSplit[0];
                this.position = lineSplit[3];
                this.time = lineSplit[4];
            }
            else
            {
                this.imsi = lineSplit[0];
                this.position = lineSplit[2];
                this.time = lineSplit[3];
            }

            //检查日期合法性
            if ( ! this.time.startsWith(date) )			//年月日必须与date一致
                throw new LineException("", -1);

            try
            {
                this.day = this.formatter.parse(this.time);
            }
            catch ( ParseException e )
            {
                throw new LineException("", 0);
            }

            //计算所属时间段
            int i = 0, n = timepoint.length;
            int hour = Integer.valueOf( this.time.split(" ")[1].split(":")[0] );
            while ( i < n && Integer.valueOf( timepoint[i] ) <= hour )
                i++;
            if ( i < n )
            {
                if ( i == 0 )
                    this.timeFlag = ( "00-" + timepoint[i] );
                else
                    this.timeFlag = ( timepoint[i-1] + "-" + timepoint[i] );
            }
            else 									//Hour大于最大的时间点
                throw new LineException("", -1);
        }

        /**
         * 输出KEY
         */
        public Text outKey()
        {
            return new Text ( this.imsi + "|" + this.timeFlag );
        }

        /**
         * 输出VALUE
         */
        public Text outValue()
        {
            long t = ( day.getTime() / 1000L );						//用时间的偏移量作为输出时间
            return new Text ( this.position + "|" + String.valueOf(t) );
        }
    }


}