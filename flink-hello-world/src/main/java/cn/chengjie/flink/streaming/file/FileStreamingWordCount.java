package cn.chengjie.flink.streaming.file;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/5 10:54
 **/
public class FileStreamingWordCount {

    public static final String fileName="D:/data/标签归因API接口文档.txt";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataSource=env.readFile(new TextInputFormat(new Path(fileName)),fileName,FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
                FilePathFilter.createDefaultFilter());

        DataStream<Tuple2<String , Integer>> dataSet=dataSource.flatMap(new RichFlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] chars=value.split("：");
                for (String s:chars){
                    if(s.length()>0){
                        collector.collect(new Tuple2<>(s,1));
                    }
                }
            }
        }).keyBy(0).sum(1).setParallelism(1);
        dataSet.print();
        env.execute("word count!");
    }
}
