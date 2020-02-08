package cn.chengjie.flink.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.math.BigDecimal;
import java.util.*;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/13 17:02
 **/
public class Kafka2EsDemo {

    public static final String TOPIC = "flink_result_list";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.251:9092,10.40.6.251:9092";

    public static final String GROUP_ID = "test-group";

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


        List<HttpHost> httpHosts =new ArrayList<>();
        httpHosts.add(new HttpHost("10.40.6.178",9200,"http"));
        httpHosts.add(new HttpHost("10.40.6.179",9200,"http"));
        httpHosts.add(new HttpHost("10.40.6.180",9200,"http"));

        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {

                        Tuple3<String,Integer,BigDecimal> data =JSONObject.parseObject(element,Tuple3.class);

                        Map result =new HashMap();
                        result.put("operation",data.f0);
                        result.put("sumVisit",data.f1.intValue());
                        result.put("avgTime",data.f2.doubleValue());


                        return Requests.indexRequest()
                                .index("sys_log_statistic")
                                .type("sys_log_statistic")
                                .source(result);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);

        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(null);
                    restClientBuilder.setMaxRetryTimeoutMillis(30000);
//                    restClientBuilder.setPathPrefix(null);
//                    restClientBuilder.setHttpClientConfigCallback(null);
                }
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(kafkaConsumer).setParallelism(4).addSink(esSinkBuilder.build());
        env.execute("sys_log_statistic job");
    }
}
