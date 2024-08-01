package org.datastream.transform.combinewater;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: hutu
 * @Date: 2024/8/1 18:21
 */
public class ConnectKeyByDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);
        SingleOutputStreamOperator<String> process = connect.process(new CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
            //定义hashmap用来存数据
            Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
            Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

            @Override
            public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                //1.存入hashmap
                Integer id = value.f0;
                if (!s1Cache.containsKey(id)) {
                    List<Tuple2<Integer, String>> s1Value = new ArrayList<>();
                    s1Value.add(value);
                    s1Cache.put(id, s1Value);
                } else {
                    s1Cache.get(id).add(value);
                }
                //2.遍历s2匹配
                if (s2Cache.containsKey(id)) {
                    for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                        out.collect("s1:" + value + "<--------->s2:" + s2Element);
                    }
                }
            }

            @Override
            public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                //1.存入hashmap
                Integer id = value.f0;
                if (!s1Cache.containsKey(id)) {
                    List<Tuple3<Integer, String, Integer>> s2Value = new ArrayList<>();
                    s2Value.add(value);
                    s2Cache.put(id, s2Value);
                } else {
                    s2Cache.get(id).add(value);
                }
                //2.遍历s1匹配
                if (s1Cache.containsKey(id)) {
                    for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                        out.collect("s1:" + s1Element + "<--------->s2:" + value);
                    }
                }
            }
        });
        process.print();


        env.execute();
    }
}
