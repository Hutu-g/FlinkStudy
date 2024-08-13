package org.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 统计每种传感器每种水位值出现的次数
 *
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class MapStateDemo {
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
        //案例需求：统计每种传感器每种水位值出现的次数
        sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {


                    MapState<Integer, Integer> vcCountState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        vcCountState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Integer>("vcCountState", Types.INT, Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = value.getVc();
                        //判断是否存在vc对应的key，有则+1，无则初始化
                        if (vcCountState.contains(vc)) {
                            Integer count = vcCountState.get(vc);
                            vcCountState.put(vc,++count);
                        } else{
                            vcCountState.put(vc,1);
                        }
                        //打印
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("传感器id为" + value.getId() + "\n");
                        for (Map.Entry<Integer, Integer> vcCount : vcCountState.entries()) {
                            outStr.append(vcCount.toString() + "\n");
                        }
                        outStr.append("==============================\n");
                        out.collect(outStr.toString());
                    }
                })
                .print();

        env.execute();
    }
}
