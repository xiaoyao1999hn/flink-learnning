package cn.chengjie.flink.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/6 9:53
 **/
public class MysqlTableApiDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv=BatchTableEnvironment.create(env);
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

        Long t1=System.currentTimeMillis();

        DataSet<Tuple3<String,Integer,Integer>>dataSet= env.createInput(jdbcInputFormat).flatMap(new FlatMapFunction<Row, Tuple3<String, Integer,Integer>>() {
            @Override
            public void flatMap(Row value, Collector<Tuple3<String, Integer,Integer>> out) throws Exception {
                out.collect(new Tuple3(value.getField(0), value.getField(1)==null?0:value.getField(1),1));
            }
        });

        TypeInformation<Tuple3<String, Integer,Long>> tpinf = new TypeHint<Tuple3<String, Integer,Long>>(){}.getTypeInfo();

        Table table = tableEnv.fromDataSet(dataSet,"operation,visit_time,visit_count");

        tableEnv.registerTable("sys_log", table);

        Table resultTable=tableEnv.sqlQuery("select operation,SUM(visit_time),COUNT(visit_count) from sys_log  where operation is not null group by operation");

        DataSet<Tuple3<String,Integer, Long>> resultSet=tableEnv.toDataSet(resultTable,tpinf);
        resultSet.print();

        TypeInformation<Tuple3<String, Integer,Long>> tpinf2 = new TypeHint<Tuple3<String, Integer,Long>>(){}.getTypeInfo();
        Table resultTable2=tableEnv.sqlQuery("select operation,AVG(visit_time),COUNT(visit_count) from sys_log  where operation is not null group by operation");
        DataSet<Tuple3<String,Integer, Long>> resultSet2=tableEnv.toDataSet(resultTable2,tpinf2);
        resultSet2.print();
        resultSet2.collect();

        Long t4=System.currentTimeMillis();
        System.out.println(t4-t1);
    }
}
