package cn.chengjie.flink.kafka;

import cn.chengjie.flink.common.domain.LogDO;
import cn.chengjie.flink.common.sink.SinkToMySQL;
import cn.chengjie.flink.kafka.schema.KafkaMsgSchema;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;

/**
 * flink run -c cn.chengjie.flink.leanning.streaming.kafka.KafkaStreamingDemo /home/hq_chengjie2/flink-demo-1.0-SNAPSHOT.jar
 * @author ChengJie
 * @desciption
 * @date 2019/12/5 11:17
 **/
@Slf4j
public class Kafka2KafkaDemo {

    public static final String TOPIC = "odsLogQueue";

    public static final String RESULT_TOPIC="flink_result_list";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.251:9092,10.40.6.251:9092";

    public static final String GROUP_ID = "test-group";

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

        DataStream<String> dataStream = env.addSource(kafkaConsumer);
        dataStream.flatMap(new FlatMapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
                List<LogDO> list =JSONArray.parseArray(value,LogDO.class);
                list.forEach(logDO->{
                    String operation = logDO.getOperation()==null?"空操作":logDO.getOperation();
                    Integer time = logDO.getTime()==null?0:logDO.getTime();
                    out.collect(new Tuple3<>(operation,time,1));
                });
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {
                value1.f1=value2.f1+value1.f1;
                value1.f2=value1.f2+value2.f2;
                return value1;
            }
        }).setParallelism(4).rebalance().flatMap(new FlatMapFunction<Tuple3<String,Integer,Integer>, Tuple3<String, Integer, Double>>() {
            @Override
            public void flatMap(Tuple3<String, Integer, Integer> value, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
                Double avg=new BigDecimal(value.f1).divide(new BigDecimal(value.f2),4,BigDecimal.ROUND_HALF_EVEN).doubleValue();
                out.collect(new Tuple3<>(value.f0,value.f2,avg));
            }
        }).setParallelism(4).flatMap(new FlatMapFunction<Tuple3<String,Integer,Double>, String>() {
            @Override
            public void flatMap(Tuple3<String, Integer, Double> value, Collector<String> out) throws Exception {
                out.collect(JSONObject.toJSONString(value));
            }
        }).addSink(new FlinkKafkaProducer<String>(
                RESULT_TOPIC,
                new KeyedSerializationSchemaWrapper<String>(new KafkaMsgSchema()),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

        env.execute("kafka test");
    }
}
