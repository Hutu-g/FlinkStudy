package org.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * DataStream 实现 wordCount：读文件有界流
 * @Author: hutu
 * @Date: 2024/7/29 16:45
 */
public class WordCountStreamDemo {
    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> lineDS = env.readTextFile("input/word.txt");
        //处理数据 切分，转换，分组，聚合，
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lineDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> wordAndOne = Tuple2.of(word, 1);
                    out.collect(wordAndOne);

                }

            }
        });
        KeyedStream<Tuple2<String, Integer>, String> wordAndOneKS = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOneKS.sum(1);
        //输出
        sum.print();
        //执行
        env.execute();

    }


}
