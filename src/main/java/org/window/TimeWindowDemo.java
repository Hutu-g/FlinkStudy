package org.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/4 3:15
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop101", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10))); //滚动窗口
//                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))); //滑动窗口
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); //会话窗口
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
                    @Override
                    public long extract(WaterSensor element) {
                        return element.getTs() * 1000L;
                    }
                })); //动态会话窗口
        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
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
