package org.datastream.transform.aggregation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datastream.bean.WaterSensor;

/**
 * @Author: hutu
 * @Date: 2024/8/1 8:50
 * 如果输入的数据是id1，只打印vc；如果输入的数据是id2，既打印ts又打印vc。
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 1L, 5),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3));
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        SingleOutputStreamOperator<WaterSensor> reduce = sensorKS.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("value1:" + value1);
                System.out.println("value2:" + value2);
                return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
            }
        });

        reduce.print();


        env.execute();
    }



}
