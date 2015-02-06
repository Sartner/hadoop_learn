package cn.sartner.hadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

/**
 * 描述:
 * 从HDFS里读取文本
 *
 * 执行方式(linux下):
 * 1. 打JAR包
 * 2. 执行命令 "${hadoop_home}/bin/hadoop jar hadoop_learn-1.0.jar cn.sartner.hadoop.hdfs.HDFS_01_ReadHDFSFile hdfs://centos1:9000/in/test1.txt"
 */
public class HDFS_01_ReadHDFSFile {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) throws  Exception{
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }

}
