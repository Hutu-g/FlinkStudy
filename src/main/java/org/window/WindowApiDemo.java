package org.window;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/3 18:53
 */
public class WindowApiDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop101", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        //1.指定窗口分配器
        //1.1 没有keyby 窗口内所有数据进入同一个子任务，并行度只能为1
        //sensorDS.windowAll();

        //1.2 有keyby的窗口  窗口按照key独立划分
        //基于时间
        WindowedStream<WaterSensor, String, TimeWindow> windowWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));//滚动窗口，窗口长度为10s
        //sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2))); //滑动窗口，窗口长度为10s 滑动步长为2s
        //sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))); //会话窗口超时间隔5s
        //基于计数
        //sensorKS.countWindow(5); //滚动窗口
        //sensorKS.countWindow(5,2); //滑动窗口 步长为2
        //sensorKS.window(GlobalWindows.create()); // 全局窗口，需要自定义触发器

        //2.指定窗口函数
        //增量聚合 来一条算一条，不输出，窗口触发输出计算结果


        //全窗口函数 数据来了不算，存起来，窗口触发计算并输出


        env.execute();

    }
}
