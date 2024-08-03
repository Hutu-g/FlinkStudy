package org.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/3 18:53
 */
public class WindowAggregateAndProcessDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop101", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        /**
         * 增量聚合 aggregate + 全窗口process
         * 1、增量聚合函数处理数据:来一条计算一条
         * 2、窗口触发时，增量聚合的结果(只有一条)传递给 全窗口函数
         * 3、经过全窗口函数的处理包装后，输出
         * 结合两者的优点:
         * 1、增量聚合:来一条计算一条，存储中间的计算结果，占用的空间少
         * 2、全窗口函数:可以通过上下文实现灵活的功能
         */
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(new MyAgg(), new MyProcess());
        aggregate.print();
        env.execute();
    }

    public static class MyAgg implements AggregateFunction<WaterSensor,Integer,String>{
        /**
         * 初始化累加器
         * @return
         */
        @Override
        public Integer createAccumulator() {
            System.out.println("创建累加器");
            return 0;
        }

        /**
         * 聚合逻辑
         * @param value The value to add
         * @param accumulator The accumulator to add the value to
         * @return
         */
        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            System.out.println("调用add方法,value=" + value);
            return accumulator + value.getVc();
        }

        /**
         * 获取最终结果，窗口触发时输出
         * @param accumulator The accumulator of the aggregation
         * @return
         */

        @Override
        public String getResult(Integer accumulator) {
            System.out.println("调用getResult方法");
            return accumulator.toString();
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            //只有会话窗口会用到
            System.out.println("调用merge方法");
            return null;
        }

    }
    //输入的数据是myagg的输出
    public static class MyProcess extends ProcessWindowFunction<String,String,String,TimeWindow>{

        /**
         *
         * @param s key
         * @param context  context
         * @param elements 此时只有一条数据
         * @param out 收集器
         * @throws Exception
         */
        @Override
        public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
            long startTS = context.window().getStart();
            long endTS = context.window().getEnd();
            String windowStart = DateFormatUtils.format(startTS, "yyyy-MM-dd HH:mm:ss.SSS");
            String windowEnd = DateFormatUtils.format(endTS, "yyyy-MM-dd HH:mm:ss.SSS");
            long count = elements.spliterator().estimateSize();
            out.collect("key = " + s + "的窗口【" + windowStart + "," + windowEnd + "】包含" + count + "条数据===》" + elements.toString());
        }
    }

}
