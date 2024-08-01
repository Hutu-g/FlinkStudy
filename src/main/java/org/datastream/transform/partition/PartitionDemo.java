package org.datastream.transform.partition;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: hutu
 * @Date: 2024/7/29 16:45
 */
public class PartitionDemo {
    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop101", 7777);
        //随机分区，random.nextInt(下游算子并行度)
        //socketDS.shuffle().print();
        //轮询，解决数据源的数据倾斜
        //socketDS.rebalance().print();
        //缩放，局部轮询，比rebalance更高效
        //socketDS.rescale().print();
        //广播，发送给下游所有子任务
        //socketDS.broadcast().print();
        //全局，只去一个下游
        //socketDS.global().print();

        env.execute();
    }
}
