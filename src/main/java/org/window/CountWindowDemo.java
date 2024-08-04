package org.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/4 3:15
 */
public class CountWindowDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop101", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, GlobalWindow> sensorWS = sensorKS
//                .countWindow(5); //滚动窗口
                .countWindow(5,2); //滑动窗口



        SingleOutputStreamOperator<String> process = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>() {

            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, GlobalWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                long maxTS = context.window().maxTimestamp();
                String maxTime = DateFormatUtils.format(maxTS, "yyyy-MM-dd HH:mm:ss.SSS");
                long count = elements.spliterator().estimateSize();
                out.collect("key = " + s + "的窗口最大时间【" + maxTime  + "】包含" + count + "条数据===》" + elements.toString());
            }
        });
        process.print();
        env.execute();
    }
}
