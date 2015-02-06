package cn.sartner.hadoop;

import java.util.StringTokenizer;

/**
 * Created by Aimee on 2015/1/5.
 */
public class Test {




    public static void main(String[] args) {





        String s1 = "2014 01 01 00    -8   -40 10122   290    60     1 -9999     0";

        StringTokenizer itr = new StringTokenizer(s1);
        String year = "";
        int temp =Integer.MIN_VALUE;
        int i=0;
        while (itr.hasMoreTokens()) {
            String s = itr.nextToken();;
            if(i==0){
                year= s;
            }else if(i==4){
                temp= Integer.parseInt(s);
                break;
            }
            i++;
        }
        System.out.println(year);
        System.out.println(temp);
    }
}
