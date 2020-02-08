package cn.chengjie.flink.kafka;

import cn.chengjie.flink.common.domain.LogDO;
import cn.chengjie.flink.common.sink.SinkToMySQL;
import cn.chengjie.flink.kafka.schema.AsyncFunction;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * flink run -c cn.chengjie.flink.leanning.streaming.kafka.KafkaStreamingDemo /home/hq_chengjie2/flink-demo-1.0-SNAPSHOT.jar
 * @author ChengJie
 * @desciption
 * @date 2019/12/5 11:17
 **/
@Slf4j
public class KafkaAsyncIO {

    public static final String TOPIC = "odsLogQueue_2";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.251:9092,10.40.6.251:9092";

    public static final String GROUP_ID = "test-group-19";

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

       final Long startTime= System.currentTimeMillis();

        DataStream async =AsyncDataStream.unorderedWait(dataStream,new AsyncFunction(),10L,TimeUnit.MINUTES);

        async.print();

//        dataStream.flatMap(new FlatMapFunction<String, LogDO>() {
//            @Override
//            public void flatMap(String value, Collector<LogDO> out) throws Exception {
//                List<LogDO> list=JSONArray.parseArray(value,LogDO.class);
//                list.forEach(log->out.collect(log));
//                long endTime = System.currentTimeMillis();
//                System.out.println("startTime: "+startTime+"\tendTime: "+endTime+"\t 耗时: "+(endTime-startTime));
//            }
//        }).print();

        env.execute("kafka test");
        }
}
