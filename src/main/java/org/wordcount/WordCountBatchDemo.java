package org.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: hutu
 * @Date: 2024/7/29 16:45
 */
public class WordCountBatchDemo {
    public static void main(String[] args) throws Exception {

        //创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取数据
        DataSource<String> lineDS = env.readTextFile("input/word.txt");

        //切分，转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    out.collect(wordTuple2);
                }
            }
        });
        //分组 0表示第一个元素
        UnsortedGrouping<Tuple2<String, Integer>> wordGroup = wordAndOne.groupBy(0);
        //分组内聚合 1表示第二个元素
        AggregateOperator<Tuple2<String, Integer>> sum = wordGroup.sum(1);
        //输出
        sum.print();

    }
}
