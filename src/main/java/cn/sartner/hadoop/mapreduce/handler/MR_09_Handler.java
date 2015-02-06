package cn.sartner.hadoop.mapreduce.handler;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

/**
 * 生成MR_09所需的数据
 */
public class MR_09_Handler {


    public static void parseFile(String FilePath,StringBuilder stringBuilder) throws IOException {
        Iterator<Element> iterator = Jsoup.parse(new File("C:\\Users\\Aimee\\Desktop\\mail_reply\\Dec 2014_1.txt"), "UTF-8").getElementById("msglist").children().get(1).children().iterator();

        File result = new File("C:\\Users\\Aimee\\Desktop\\mail_reply\\Dec 2014_1.txt");

        while (iterator.hasNext()){
            Element e = iterator.next();
            String author = e.getElementsByClass("author").get(0).text();
            if(StringUtils.isEmpty(author))continue;
            String subject = e.getElementsByClass("subject").get(0).getElementsByTag("a").text();
            if(StringUtils.isEmpty(subject))continue;
            String date = e.getElementsByClass("date").get(0).text().replaceAll("Dec,", "12").replaceAll("Nov,","11").substring(5);
            if(StringUtils.isEmpty(date))continue;

            try {
                date = String.valueOf(DateUtils.parseDate("2015 " + date, new String[]{"yyyy dd MM HH:mm"}).getTime());
            } catch (ParseException e1) {
                e1.printStackTrace();
            }

            stringBuilder.append(author).append("$$").append(subject).append("$$").append(date).append(System.getProperty("line.separator"));
        }
    }


    public static void main(String[] args) throws Exception {





        StringBuilder sb = new StringBuilder();
        parseFile("C:\\Users\\Aimee\\Desktop\\mail_reply\\Dec 2014_1.txt",sb);
        parseFile("C:\\Users\\Aimee\\Desktop\\mail_reply\\Dec 2014_2.txt",sb);
        parseFile("C:\\Users\\Aimee\\Desktop\\mail_reply\\Nov 2014_1.txt",sb);
        parseFile("C:\\Users\\Aimee\\Desktop\\mail_reply\\Nov 2014_2.txt",sb);
        parseFile("C:\\Users\\Aimee\\Desktop\\mail_reply\\Nov 2014_3.txt",sb);
        FileUtils.writeStringToFile(new File("C:\\Users\\Aimee\\Desktop\\mail_reply\\MR_09_MailReply.data"),sb.toString());



    }

}
