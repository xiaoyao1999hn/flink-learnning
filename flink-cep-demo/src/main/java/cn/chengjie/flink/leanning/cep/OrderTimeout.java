package cn.chengjie.flink.leanning.cep;

import cn.chengjie.flink.leanning.cep.entity.OrderEvent;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ChengJie
 * @desciption
 * @date 2019/12/17 15:22
 **/
@Slf4j
public class OrderTimeout {

    public static class DataSource implements Iterator<OrderEvent>, Serializable {
        private final AtomicInteger atomicInteger = new AtomicInteger(0);
        private final List<OrderEvent> orderEventList = Arrays.asList(
                new OrderEvent("1", "create"),
                new OrderEvent("2", "create"),
                new OrderEvent("2", "pay")
        );

        @Override
        public boolean hasNext() {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }

        @Override
        public OrderEvent next() {
            return orderEventList.get(atomicInteger.getAndIncrement() % 3);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderEvent> loginEventStream = env.fromCollection(new DataSource(), OrderEvent.class);

        Pattern<OrderEvent, OrderEvent> loginFailPattern = Pattern.<OrderEvent>
                begin("begin")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("create");
                    }
                })
                .next("next")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent loginEvent, Context context) throws Exception {
                        return loginEvent.getType().equals("pay");
                    }
                })
                .within(Time.seconds(1));

        PatternStream<OrderEvent> patternStream = CEP.pattern(
                loginEventStream.keyBy(OrderEvent::getUserId),
                loginFailPattern);

//        AfterMatchSkipStrategy.noSkip();

        OutputTag<OrderEvent> orderTiemoutOutput = new OutputTag<OrderEvent>("orderTimeout") {
        };

        SingleOutputStreamOperator<OrderEvent> complexResult = patternStream.select(
                orderTiemoutOutput,
                new PatternTimeoutFunction<OrderEvent, OrderEvent>() {
                    @Override
                    public OrderEvent timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                        log.info("size :{} ,OrderEventList: {} , l: {}",map.get("begin").size(),JSONObject.toJSONString(map.get("begin")),l);
                        return new OrderEvent("timeout", map.get("begin").get(0).getUserId());
                    }
                },
                new PatternSelectFunction<OrderEvent, OrderEvent>() {
                    @Override
                    public OrderEvent select(Map<String, List<OrderEvent>> map) throws Exception {
                        log.info("OrderEventList: {} ",JSONObject.toJSONString(map.get("next")));
                        return new OrderEvent("success", map.get("next").get(0).getUserId());
                    }
                }
        );

        DataStream<OrderEvent> timeoutResult = complexResult.getSideOutput(orderTiemoutOutput);

        complexResult.print();
        timeoutResult.print();

        env.execute();

    }

}