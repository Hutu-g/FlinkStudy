package org.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/3 18:53
 */
public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop101", 7777).map(new WaterSensorMapFunction());
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(r -> r.getId());

        //1.窗口分配器
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //2.窗口函数，增量聚合reduce
        /**
         * 窗口的reduce
         * 1、相同的key的第一条数据来的时候不会调用reduce
         * 2、增量聚合 来一条算一条，不输出，窗口触发输出计算结果
         *  全窗口函数 数据来了不算，存起来，窗口触发计算并输出
         */
//        SingleOutputStreamOperator<WaterSensor> reduce = sensorWS.reduce(new ReduceFunction<WaterSensor>() {
//            @Override
//            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
//                System.out.println("调用reduce方法，之前的结果:" + value1 + ",现在来的数据:" + value2);
//                return new WaterSensor(value1.getId(), System.currentTimeMillis(), value1.getVc() + value2.getVc());
//            }
//        });
//        reduce.print();
        /**
         * 窗口的aggregate
         * 1、AggregateFunction<IN, ACC, OUT> 输出数据类型，累加器类型，输出数据类型
         * 2、增量聚合 来一条算一条，不输出，窗口触发输出计算结果
         */
        SingleOutputStreamOperator<String> aggregate = sensorWS.aggregate(new AggregateFunction<WaterSensor, Integer, String>() {
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
        });
        aggregate.print();


        env.execute();
    }

}
