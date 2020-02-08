package cn.chengjie.flink.kafka.schema;

import cn.chengjie.flink.common.domain.LogDO;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/19 15:28
 **/
@Slf4j
public class AsyncFunction extends RichAsyncFunction<String,LogDO> {

    ExecutorService executorService;

    final long startTime=System.currentTimeMillis();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
//        executorService= new ThreadPoolExecutor(4,10,10L,TimeUnit.SECONDS,new ArrayBlockingQueue(10));
    }

    @Override
    public void close() throws Exception {
        super.close();
//        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<LogDO> resultFuture) throws Exception {
        List<LogDO> list=JSONArray.parseArray(input,LogDO.class);
        long endTime = System.currentTimeMillis();
        System.out.println("startTime: "+startTime+"\tendTime: "+endTime+"\t 耗时: "+(endTime-startTime));
        resultFuture.complete(list);
    }

    @Override
    public void timeout(String input, ResultFuture<LogDO> resultFuture) throws Exception {
        log.error("time out for [{}]",input);
    }
}
