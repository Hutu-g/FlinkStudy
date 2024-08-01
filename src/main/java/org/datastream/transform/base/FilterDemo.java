package org.datastream.transform.base;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.datastream.bean.WaterSensor;

/**
 * @Author: hutu
 * @Date: 2024/8/1 8:50
 */
public class FilterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("id1", 1L, 1),
                new WaterSensor("id2", 2L, 2),
                new WaterSensor("id3", 3L, 3));

//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunction<WaterSensor>() {
//            @Override
//            public boolean filter(WaterSensor value) throws Exception {
//                return "id1".equals(value.getId());
//            }
//        });

//        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter((FilterFunction<WaterSensor>) value -> "id1".equals(value.getId()));
        SingleOutputStreamOperator<WaterSensor> filter = sensorDS.filter(new FilterFunctionImpl("id1"));


        filter.print();

        env.execute();
    }
    public static class FilterFunctionImpl implements FilterFunction<WaterSensor> {
        private String id;

        FilterFunctionImpl(String id) { this.id=id; }

        @Override
        public boolean filter(WaterSensor value) throws Exception {
            return this.id.equals(value.id);
        }
    }

}
