package cn.chengjie.flink.tags;

import cn.chengjie.flink.tags.domain.TagInfoDO;
import cn.chengjie.flink.tags.utils.SqlUtils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.*;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/15 17:24
 **/
public class TagsJob {

    public static final String TOPIC = "fx_goods_label_topic";

    public static final String SERVERS = "10.40.6.251:9092,10.40.6.252:9092,10.40.6.253:9092";

    public static final String GROUP_ID = "group-2";

    public static final String DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TypeInformation<Tuple6<String, String, String, String, String, Double>> tpynf = new TypeHint<Tuple6<String, String, String, String, String, Double>>(){}.getTypeInfo();

        Properties properties = new Properties();
        properties.put("bootstrap.servers", SERVERS);
        properties.put("group.id", GROUP_ID);
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", 10000);
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", 60000);
        properties.put("key.deserializer", DESERIALIZER);
        properties.put("value.deserializer", DESERIALIZER);

        FlinkKafkaConsumer kafkaConsumer = new FlinkKafkaConsumer(TOPIC, new SimpleStringSchema(), properties);

        DataStream<Tuple6<String,String,String,String,String,Double>> data=env.addSource(kafkaConsumer)
                .setParallelism(4)
                .flatMap(new FlatMapFunction<String, Tuple6<String,String,String,String,String,Double>>() {
            @Override
            public void flatMap(String value, Collector<Tuple6<String, String, String, String, String, Double>> out) throws Exception {
                List<JSONObject> list =JSONArray.parseArray(value,JSONObject.class);
                list.forEach(tag->out.collect(new Tuple6<>(
                        tag.getString("goods_sn"),tag.getString("bu_code"), tag.getString("bu_name"),
                        tag.getString("lable_code"), tag.getString("lable_name"), tag.getDouble("lable_value")
                        )
                ));
            }
        }).setParallelism(4);

        tableEnv.registerDataStream("dm_dss_fx_goods_label_f",data,"sku,bu_code,bu_name,label_code,label_name,label_value");

        Table t1=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL0));

        Table t2=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL1));

        Table t3=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL2));

        Table t4=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL3));

        Table t5=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL4));

        Table t6=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL5));

        Table t7=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL6));

        Table t8=tableEnv.sqlQuery(String.format(SqlUtils.SQL_TEMPLATE,SqlUtils.SQL7));

        List<Table> list=new ArrayList<>();
        list.add(t1);list.add(t2);
        list.add(t3);list.add(t4);
        list.add(t5);list.add(t6);
        list.add(t7);list.add(t8);

        final String[] ruleNames = {"重点款","热销款","未上架","新品","滞销款","呆滞款","平销款","无动销"};

        final String[] ruleCodes = {"WAFGMSDB","8TRIMPID","XTFVYNY9","AEH0SUJT","0QL6ZII4","7NIALQDC","U3AASTBF","QETC8DOT"};

        for(int i=0;i<list.size();i++){
            final int index=i;
            tableEnv.toRetractStream(list.get(i),tpynf).flatMap(new FlatMapFunction<Tuple2<Boolean,Tuple6<String,String,String,String,String,Double>>, TagInfoDO>() {
                @Override
                public void flatMap(Tuple2<Boolean, Tuple6<String, String, String, String, String, Double>> value, Collector<TagInfoDO> out) throws Exception {
                    if(value.f0){
                        out.collect(new TagInfoDO(value.f1.f0,value.f1.f1,value.f1.f2,ruleNames[index],ruleCodes[index],"20191201",value.f1.f5));
                    }
                }
            }).setParallelism(4).addSink(getEsSink());
        }

        env.execute("tags job !");
    }


    public static ElasticsearchSink<TagInfoDO> getEsSink(){
        List<HttpHost> httpHosts =new ArrayList<>();
        httpHosts.add(new HttpHost("10.40.6.178",9200,"http"));
        httpHosts.add(new HttpHost("10.40.6.179",9200,"http"));
        httpHosts.add(new HttpHost("10.40.6.180",9200,"http"));

        ElasticsearchSink.Builder<TagInfoDO> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<TagInfoDO>() {
                    public IndexRequest createIndexRequest(TagInfoDO element) {

                        Map result = new HashMap();

                        result.put("sku",element.getSku());
                        result.put("bu_code",element.getBuCode());
                        result.put("bu_name",element.getBuName());
                        result.put("code",element.getCode());
                        result.put("rule_name",element.getRuleName());
                        result.put("the_date_cd",element.getTheDateCd());

                        return Requests.indexRequest()
                                .index("product_tag_info")
                                .type("product_tag_info")
                                .source(result);
                    }
                    @Override
                    public void process(TagInfoDO element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> restClientBuilder.setMaxRetryTimeoutMillis(30000)
        );
        return esSinkBuilder.build();
    }

}
