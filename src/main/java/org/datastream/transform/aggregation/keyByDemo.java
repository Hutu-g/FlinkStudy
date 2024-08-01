package org.datastream.transform.aggregation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;

/**
 * @Author: hutu
 * @Date: 2024/8/1 8:50
 * 如果输入的数据是id1，只打印vc；如果输入的数据是id2，既打印ts又打印vc。
 */
public class keyByDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3));

        /**
         * 按照id分组
         *  要点：
         *  1.返回的是 KeyedStream 键控流
         *  2.Key不是 转换算子 ，不能重分区 不能设置并行度
         */
        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });


        sensorKS.print();


        env.execute();
    }



}
