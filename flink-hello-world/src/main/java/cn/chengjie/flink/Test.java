package cn.chengjie.flink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/23 10:23
 **/
public class Test {
    public static void main(String[] args) {
        String list="{key: [a,b,c,d,e]}";



        JSONObject temp =JSONObject.parseObject(list);

        System.out.println(temp.toString());
    }
}
