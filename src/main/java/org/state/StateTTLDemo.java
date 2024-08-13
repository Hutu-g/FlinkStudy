package org.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

import java.time.Duration;

/**
 * 针对每种传感器输出最高的3个水位值
 *
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class StateTTLDemo {
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
        sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    ValueState<Integer> lastVcState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.seconds(10)) //过期时间
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //状态创建和写入 更新过期时间
                                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) //不返回过期的状态值
                                .build();
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                        stateDescriptor.enableTimeToLive(stateTtlConfig);

                        lastVcState = getRuntimeContext().getState(stateDescriptor);
                    }
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //取出上一条数据的水为值
                        int lastVc = lastVcState.value() == null ? 0 : lastVcState.value();
                        int vc = value.getVc();
                        if (Math.abs(vc - lastVc) > 10) {
                            out.collect("传感器=" + value.getId() + "==>当前水位值=" + vc + ",与上一条水位值=" + lastVc + ",相差超过10！！！！");
                        }
                        lastVcState.update(vc);
                    }
                })
                .print();

        env.execute();
    }
}
