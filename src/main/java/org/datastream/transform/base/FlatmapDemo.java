package org.datastream.transform.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;

/**
 * @Author: hutu
 * @Date: 2024/8/1 8:50
 * 如果输入的数据是id1，只打印vc；如果输入的数据是id2，既打印ts又打印vc。
 */
public class FlatmapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("id1", 1L, 1),
                new WaterSensor("id2", 2L, 2),
                new WaterSensor("id3", 3L, 3));

//        SingleOutputStreamOperator<String> flatted = sensorDS.flatMap((FlatMapFunction<WaterSensor, String>) (value, out) -> {
//            if ("id1".equals(value.getId()))
//                out.collect(value.getVc().toString());
//            else if ("id2".equals(value.getId())) {
//                out.collect(value.getTs().toString());
//                out.collect(value.getVc().toString());
//            }
//
//        });
        SingleOutputStreamOperator<String> flatted = sensorDS.flatMap(new FlatMapFunction<WaterSensor, String>() {
            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                if ("id1".equals(value.getId()))
                    out.collect(value.getVc().toString());
                else if ("id2".equals(value.getId())) {
                    out.collect(value.getTs().toString());
                    out.collect(value.getVc().toString());
                }

            }
        });


        flatted.print();

        env.execute();
    }



}
