package cn.chengjie.flink.jdbc.mysql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/3 14:29
 **/

public class MysqlDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes = new TypeInformation[]{
                STRING_TYPE_INFO, INT_TYPE_INFO
        };

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDBUrl("jdbc:mysql://10.40.6.180:3306/global_bi?useUnicode=true&characterEncoding=utf-8&autoReconnect=true")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("select operation,time from  sys_log")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        long bgTime=System.currentTimeMillis();
        DataSet<Tuple3<String, Integer,Double>> dataSource = env.createInput(jdbcInputFormat).flatMap(new FlatMapFunction<Row, Tuple3<String, Integer,Integer>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, Integer,Integer>> out) throws Exception {
                String operation = value.getField(0)==null?"空操作":value.getField(0).toString();
                Integer time = value.getField(1)==null?0:Integer.parseInt(value.getField(1).toString());
                out.collect(new Tuple3(operation, time,1));

            }
        }).groupBy(0).reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {
                value1.f1=value2.f1+value1.f1;
                value1.f2=value1.f2+value2.f2;
                return value1;
            }
        }).flatMap(new FlatMapFunction<Tuple3<String,Integer,Integer>, Tuple3<String, Integer, Double>>() {
            @Override
            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
                Double avg=new BigDecimal(value.f1).divide(new BigDecimal(value.f2),4,BigDecimal.ROUND_HALF_EVEN).doubleValue();
                out.collect(new Tuple3<>(value.f0,value.f2,avg));
            }
        });
        dataSource.print();
        long endTime=System.currentTimeMillis();
        System.out.println("总计耗时："+(endTime-bgTime));
    }
}
