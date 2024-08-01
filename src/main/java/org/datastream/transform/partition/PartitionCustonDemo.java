package org.datastream.transform.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hutu
 * @Date: 2024/7/29 16:45
 */
public class PartitionCustonDemo {
    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop101", 7777);

        socketDS.partitionCustom(new MyPartitioner(),r->r).print();

        env.execute();
    }

    public static class MyPartitioner implements Partitioner<String>{
        @Override
        public int partition(String key, int numPartitions) {
            return Integer.parseInt(key) % numPartitions;
        }
    }
}
