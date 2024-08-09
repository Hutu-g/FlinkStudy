package org.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

import java.time.Duration;

/**
 * 按key
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class KeyedProcessTimeDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop101", 7777)
                .map(new WaterSensorMapFunction());
        //定义watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) //乱序watermark，等待3s
                //指定时间戳分配器，从数据中获取
                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                    System.out.println("数据 = " + element + "recordTs" + recordTimestamp);
                    return element.getTs() * 1000L;
                });

        SingleOutputStreamOperator<WaterSensor> watermarks = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> sensorKS = watermarks.keyBy(r -> r.getId());

        SingleOutputStreamOperator<String> process = sensorKS.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                //定时器
                TimerService timerService = ctx.timerService();
                //注册定时器，事件时间
//                Long currentEventTime = ctx.timestamp();//数据中提取时间
//                timerService.registerEventTimeTimer(5000L);
//                System.out.println("当前时间是：" + currentEventTime + ",注册了一个5s的定时器");
                //注册定时器，处理时间
//                long currentTs = timerService.currentProcessingTime();
//                timerService.registerProcessingTimeTimer(currentTs + 5000L);
//                System.out.println("当前时间是：" + currentTs + ",注册了一个5s后的定时器");
                //获取process的水位线
                long currentWatermark = timerService.currentWatermark();
                System.out.println("当前数据是：" + value + ",当前Watermark" + currentWatermark);
//

                //删除定时器
                //timerService.deleteEventTimeTimer();
                //timerService.deleteProcessingTimeTimer();
                //获取当前处理时间 == 系统时间
//                long currentTs = timerService.currentProcessingTime();
//                long wm = timerService.currentWatermark();
            }

            /**
             * 定时器触发
             * @param timestamp 当时间进展
             * @param ctx 上下文
             * @param out 采集器
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                System.out.println("当前时间是：" + timestamp + ",定时器触发");
            }
        });


        env.execute();
    }
}
