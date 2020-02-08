package cn.chengjie.flink.leanning.cep;

import cn.chengjie.flink.leanning.cep.entity.LogDO;
import com.alibaba.fastjson.JSONArray;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/17 15:20
 **/
public class FlinkCepDemo {

    public static final String TOPIC = "odsLogQueue_2";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.251:9092,10.40.6.251:9092";

    public static final String GROUP_ID = "test-group-2";

    public static final String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        DataStream<LogDO> dataStream = env.addSource(kafkaConsumer).flatMap(new FlatMapFunction<String, LogDO>() {
            @Override
            public void flatMap(String value, Collector<LogDO> out) throws Exception {
                List<LogDO> list = JSONArray.parseArray(value, LogDO.class);
                Optional.ofNullable(list).ifPresent(records -> records.forEach(record -> out.collect(record)));
            }
        });

//        Pattern<LogDO, LogDO> pattern = Pattern.<LogDO>
//                begin("begin")
//                .followedBy("green").where(new IterativeCondition<LogDO>() {
//                    @Override
//                    public boolean filter(LogDO logDO, Context context) throws Exception {
//                        return logDO.getTime() == null ? true : logDO.getTime() < 100;
//                    }
//                })
//                .followedBy("yellow").where(new IterativeCondition<LogDO>() {
//                    @Override
//                    public boolean filter(LogDO logDO, Context context) throws Exception {
//                        return logDO.getTime() == null ? false : ((logDO.getTime() > 100) && (logDO.getTime() <= 500));
//                    }
//                })
//                .followedBy("red").where(new IterativeCondition<LogDO>() {
//                    @Override
//                    public boolean filter(LogDO logDO, Context<LogDO> context) throws Exception {
//                        return logDO.getTime() == null ? false : ((logDO.getTime() > 500) && (logDO.getTime() <= 1000));
//                    }
//                })
//                .followedBy("danger").where(new IterativeCondition<LogDO>() {
//                    @Override
//                    public boolean filter(LogDO logDO, Context<LogDO> context) throws Exception {
//                        return logDO.getTime() == null ? false : (logDO.getTime() > 1000);
//                    }
//                });

        Pattern<LogDO, LogDO> yellowPattern = Pattern.<LogDO>begin("data")
                .where(new SimpleCondition<LogDO>() {
                    @Override
                    public boolean filter(LogDO value) throws Exception {
                        if (value.getTime() == null) {
                            value.setTime(99999);
                        }
                        value.setParams("green");
                        return (value.getTime() < 100);
                    }
                }).or(new SimpleCondition<LogDO>() {
                    @Override
                    public boolean filter(LogDO value) throws Exception {
                        if (value.getTime() == null) {
                            value.setTime(99999);
                        }
                        value.setParams("yellow");
                        return (value.getTime() > 100) && (value.getTime() <= 500);
                    }
                }).or(new SimpleCondition<LogDO>() {
                    @Override
                    public boolean filter(LogDO value) throws Exception {
                        if (value.getTime() == null) {
                            value.setTime(99999);
                        }
                        value.setParams("red");
                        return (value.getTime() > 500) && (value.getTime() <= 10000);
                    }
                }).or(new SimpleCondition<LogDO>() {
                    @Override
                    public boolean filter(LogDO value) throws Exception {
                        if (value.getTime() == null) {
                            value.setTime(99999);
                        }
                        value.setParams("exception");
                        return value.getTime() > 10000;
                    }
                });

        CEP.pattern(dataStream, yellowPattern).select(new PatternSelectFunction<LogDO, LogDO>() {
            @Override
            public LogDO select(Map<String, List<LogDO>> map) throws Exception {
                return map.get("data").get(0);
            }
        }).flatMap(new FlatMapFunction<LogDO, Tuple4<String, Integer, Integer, String>>() {
            @Override
            public void flatMap(LogDO value, Collector<Tuple4<String, Integer, Integer, String>> out) throws Exception {
                out.collect(new Tuple4<>(value.getOperation(), value.getTime(), value.getType(), value.getParams()));
            }
        }).writeAsText("d://data/red").setParallelism(1);

        env.execute("CEP DEMO");
    }
}
