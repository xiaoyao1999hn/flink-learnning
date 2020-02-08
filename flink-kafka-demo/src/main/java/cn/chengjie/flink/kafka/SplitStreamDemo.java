package cn.chengjie.flink.kafka;

import cn.chengjie.flink.common.domain.LogDO;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/15 11:52
 **/
public class SplitStreamDemo {

    public static final String TOPIC = "fx_goods_label_topic";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.251:9092,10.40.6.251:9092";

    public static final String GROUP_ID = "group-2";

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
        SplitStream<Tuple3<String, Integer, Integer>> splitStream = env.addSource(kafkaConsumer)
                .setParallelism(4)
                .flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                        List<JSONObject> list =JSONArray.parseArray(value,JSONObject.class);
                        list.forEach(logDO -> {
                            out.collect(new Tuple3<>(logDO.getString("label_code"), logDO.getInteger("label_value"),2));
                        });
                    }
                })
                .setParallelism(4)
                .split(new OutputSelector<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Iterable<String> select(Tuple3<String, Integer, Integer> value) {
                        List<String> output = new ArrayList<>();
                        switch (value.f2) {
                            case 0:
                                output.add("DEVELOPMENT");
                                break;
                            case 1:
                                output.add("GLOBAL_BI_LOG");
                                break;
                            case 2:
                                output.add("TABLEAU_LOG");
                                break;
                            default:
                                output.add("DEVELOPMENT");
                        }
                        return output;
                    }
                });

        splitStream.select("TABLEAU_LOG").print();


        env.execute("splitSteam demo");
    }
}
