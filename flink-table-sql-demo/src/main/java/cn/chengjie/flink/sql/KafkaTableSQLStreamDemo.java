package cn.chengjie.flink.sql;

import cn.chengjie.flink.common.domain.LogDO;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import java.util.List;
import java.util.Properties;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/15 11:52
 **/
public class KafkaTableSQLStreamDemo {

    public static final String TOPIC = "odsLogQueue";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.251:9092,10.40.6.251:9092";

    public static final String GROUP_ID = "test-group-10";

    public static final String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", SERVERS);
        properties.put("group.id", GROUP_ID);
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", 1000);
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", 6000);
        properties.put("key.deserializer", DESERIALIZER);
        properties.put("value.deserializer", DESERIALIZER);
        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer(TOPIC, new SimpleStringSchema(), properties);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple3<String, Integer, Integer>> splitStream = env.addSource(kafkaConsumer)
                .setParallelism(4)
                .flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        List<LogDO> list = JSONArray.parseArray(value, LogDO.class);
                        list.forEach(logDO -> {
                            String operation = logDO.getOperation() == null ? "空操作" : logDO.getOperation();
                            Integer time = logDO.getTime() == null ? 0 : logDO.getTime();
                            out.collect(new Tuple3<>(operation, time, logDO.getType()));
                        });
                    }
                });

        tableEnv.registerDataStream("sys_log",splitStream,"operation,visit_time,menu_type");

        Table t1=tableEnv.sqlQuery("select operation,count(operation),avg(visit_time) from sys_log where visit_time>50 group by operation");
        TypeInformation<Tuple3<String, Long,Integer>> tpinf = new TypeHint<Tuple3<String, Long,Integer>>(){}.getTypeInfo();
        DataStream<Tuple3<String, Long,Integer>> s1=tableEnv.toRetractStream(t1,tpinf).flatMap(new FlatMapFunction<Tuple2<Boolean, Tuple3<String, Long, Integer>>, Tuple3<String, Long, Integer>>() {
            @Override
            public void flatMap(Tuple2<Boolean, Tuple3<String, Long, Integer>> value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                if(value.f0){
                    out.collect(value.f1);
                }
            }
        });

        Table t2=tableEnv.sqlQuery("select operation,count(operation),avg(visit_time) from sys_log where visit_time>100 group by operation");
        DataStream<Tuple3<String, Long,Integer>> s2=tableEnv.toRetractStream(t2,tpinf).flatMap(new FlatMapFunction<Tuple2<Boolean, Tuple3<String, Long, Integer>>, Tuple3<String, Long, Integer>>() {
            @Override
            public void flatMap(Tuple2<Boolean, Tuple3<String, Long, Integer>> value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                if(value.f0){
                    out.collect(value.f1);
                }
            }
        });

        s1.connect(s2).flatMap(new CoFlatMapFunction<Tuple3<String,Long,Integer>, Tuple3<String,Long,Integer>, Tuple3<String,Long,Integer>>() {
            @Override
            public void flatMap1(Tuple3<String, Long, Integer> value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void flatMap2(Tuple3<String, Long, Integer> value, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                out.collect(value);
            }
        });
//                .writeAsText("D://data/result").setParallelism(1);
//        t1.union(t2).

        s1.print();
        env.execute("splitSteam demo");
    }
}
