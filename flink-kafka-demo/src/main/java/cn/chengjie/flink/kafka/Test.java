package cn.chengjie.flink.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/20 8:44
 **/
public class Test {
    public static void main(String[] args) {
        Long t= 1570503443000L;
        Date date = new Date(t);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        System.out.println(sdf.format(date));

    }
}
