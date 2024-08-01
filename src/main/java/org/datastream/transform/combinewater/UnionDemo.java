package org.datastream.transform.combinewater;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hutu
 * @Date: 2024/8/1 18:21
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> ds2 = env.fromElements(11, 22, 33);
        DataStreamSource<String> ds3 = env.fromElements("111", "222", "333");

        ds1.union(ds2,ds3.map(Integer::valueOf))
                .print();

        env.execute();
    }
}
