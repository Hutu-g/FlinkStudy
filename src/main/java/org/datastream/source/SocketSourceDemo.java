package org.datastream.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hutu
 * @Date: 2024/7/31 15:46
 */
public class SocketSourceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop101", 7777);

        socketDS.print();


        env.execute();
    }

}
