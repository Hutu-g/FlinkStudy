package org.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

import java.time.Duration;
import java.util.Map;

/**
 * 计算每种传感器的水位和
 *
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop101", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, ts) -> element.getTs() * 1000L)
                );
        //案例需求：计算每种传感器的水位和
        sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sumVcState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("sumVcState", (v1, v2) -> v1 + v2, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        sumVcState.add(value.getVc());
                        Integer sum = sumVcState.get();
                        out.collect("传感器id为" + value.getId() + ",水位值总和为" + sum);
                    }
                }).print();

        env.execute();
    }
}
