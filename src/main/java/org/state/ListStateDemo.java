package org.state;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

/**
 * 检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警。
 *
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class ListStateDemo {
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
        //案例需求：检测每种传感器的水位值，如果连续的两个水位值超过10，就输出报警。
        sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    ListState<Integer> vcListState;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        vcListState =  getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcListState",Types.INT));
                    }
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //来一条，存到list状态里
                        vcListState.add(value.getVc());
                        // 从list状态拿出来,排序，只留3个最大的
                        Iterable<Integer> vcListIt = vcListState.get();
                        List<Integer> vcList = new ArrayList<>();
                        for (Integer vc : vcListIt) {
                            vcList.add(vc);
                        }
                        vcList.sort((o1, o2) -> o2-o1);
                        if (vcList.size() > 3){
                            vcList.remove(3);
                        }
                        out.collect("传感器id为" + value.getId() + ",最大的3个水位值=" + vcList);
                        //更新list状态
                        vcListState.update(vcList);
                    }
                })
                .print();

        env.execute();
    }
}
