package org.datastream.transform.combinewater;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author: hutu
 * @Date: 2024/8/1 18:21
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
        DataStreamSource<String> ds2 = env.fromElements("a", "b", "c");
        ConnectedStreams<Integer, String> connect = ds1.connect(ds2);
        SingleOutputStreamOperator<String> result = connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(String value) throws Exception {
                return value;
            }
        });
        result.print();
        env.execute();
    }
}
