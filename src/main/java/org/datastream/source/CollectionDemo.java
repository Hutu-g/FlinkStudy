package org.datastream.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 *
 * @Author: hutu
 * @Date: 2024/7/31 15:29
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source = env
                  .fromElements(1,22,3);
//                .fromCollection(Arrays.asList(1, 22, 3));
        source.print();
        env.execute();
    }
}
