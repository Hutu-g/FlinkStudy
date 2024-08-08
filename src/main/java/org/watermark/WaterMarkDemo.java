package org.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class WaterMarkDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop101", 7777)
                .map(new WaterSensorMapFunction());
        //定义watermark策略
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forMonotonousTimestamps() //升序watermark，没有等待时间
                //指定时间戳分配器，从数据中获取
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        System.out.println("数据 = " + element + "recordTs" + recordTimestamp);
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> watermarks = sensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> sensorKS = watermarks.keyBy(r -> r.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            /**
             *  2.全窗口函数 数据来了不算，存起来，窗口触发计算并输出
             * @param s 分组的key
             * @param context 上下文
             * @param elements 存的数据
             * @param out 采集器
             * @throws Exception
             */
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long startTS = context.window().getStart();
                long endTS = context.window().getEnd();
                String windowStart = DateFormatUtils.format(startTS, "yyyy-MM-dd HH:mm:ss.SSS");
                String windowEnd = DateFormatUtils.format(endTS, "yyyy-MM-dd HH:mm:ss.SSS");
                long count = elements.spliterator().estimateSize();
                out.collect("key = " + s + "的窗口【" + windowStart + "," + windowEnd + ")包含" + count + "条数据===》" + elements.toString());
            }
        });
        process.print();
        env.execute();
    }
}
