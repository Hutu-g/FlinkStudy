package org.datastream.transform.base;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datastream.bean.WaterSensor;

/**
 * @Author: hutu
 * @Date: 2024/8/1 8:27
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("id1", 1L, 1),
                new WaterSensor("id2", 2L, 2),
                new WaterSensor("id3", 3L, 3));

        //方式1
        SingleOutputStreamOperator<String> map = sensorDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return value.getId();
            }
        });
        //方式2Lambda表达式 处理语句只有一个的时候可以使用Lambda表达式
        //SingleOutputStreamOperator<String> map = sensorDS.map((MapFunction<WaterSensor, String>) value -> value.getId());
        map.print();

        env.execute();
    }

    //方式3 定义类
    public static class MyMapFunction implements MapFunction<WaterSensor, String>{

        @Override
        public String map(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
