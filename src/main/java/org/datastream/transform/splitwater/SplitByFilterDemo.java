package org.datastream.transform.splitwater;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hutu
 * @Date: 2024/7/29 16:45
 */
public class SplitByFilterDemo {
    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //读取数据
        //读取一个整数数字流，将数据流划分为奇数流和偶数流。
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop101", 7777);
        socketDS.filter(value -> Integer.parseInt(value) % 2 == 0).print("偶数流");
        socketDS.filter(value -> Integer.parseInt(value) % 2 == 1).print("奇数流");
        env.execute();
    }
}
