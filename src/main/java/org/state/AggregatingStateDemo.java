package org.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

import java.time.Duration;

/**
 * 计算每种传感器的平均水位
 *
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class AggregatingStateDemo {
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
        //案例需求：计算每种传感器的平均水位
        sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    AggregatingState<Integer,Double> avgVcState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        avgVcState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("avgVcState", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                            @Override
                            public Tuple2<Integer, Integer> createAccumulator() {
                                return Tuple2.of(0, 0);
                            }

                            @Override
                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                            }

                            @Override
                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                return accumulator.f0 * 1D / accumulator.f1;
                            }

                            @Override
                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                return null;
                            }
                        }, Types.TUPLE(Types.INT, Types.INT)));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        avgVcState.add(value.getVc());
                        Double vcAvg = avgVcState.get();
                        out.collect("传感器id为" + value.getId() + ",平均水位值为" + vcAvg);
                    }
                }).print();

        env.execute();
    }
}
