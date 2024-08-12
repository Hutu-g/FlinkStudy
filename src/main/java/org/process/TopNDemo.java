package org.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.datastream.bean.WaterSensor;
import org.datastream.bean.WaterSensorMapFunction;

import java.time.Duration;
import java.util.*;

/**
 * 使用windowAll+process
 * 缺点，并行度1效率低
 * @Author: hutu
 * @Date: 2024/8/4 14:39
 */
public class TopNDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop101", 7777)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, ts) -> element.getTs() * 1000L)
                );
        //最近10s == 窗口10s， 每5s输出 == 滑动步长
        //todo 思路1：所有数据到一起，用hashmap存，key = vc ，value = count值
        sensorDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MyTopNPAWF())
                .print();

        env.execute();
    }

    public static class MyTopNPAWF extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {


        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {

            //用hashmap存，key = vc ，value = count值
            Map<Integer, Integer> vcCountMap = new HashMap<>();
            //1.遍历数据，统计各个vc出现的次数
            for (WaterSensor element : elements) {
                Integer vc = element.getVc();
                if (vcCountMap.containsKey(vc)) {
                    //如果key存在，不是这个key的第一条数据，进行累加
                    vcCountMap.put(vc, vcCountMap.get(vc) + 1);
                } else {
                    //如果key不在，初始化key和v
                    vcCountMap.put(vc, 1);
                }
            }
            //2.对count值进行排序
            List<Tuple2<Integer, Integer>> datas = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                datas.add(Tuple2.of(vc, vcCountMap.get(vc)));
            }
            //后减前，降序
            datas.sort((o1, o2) -> o2.f1 - o1.f1);

            //3.取count中最大的两个
            StringBuilder outStr = new StringBuilder();
            outStr.append("==========================");
            //遍历排序后的list 取出两个
            for (int i = 0; i < Math.min(2, datas.size()); i++) {
                Tuple2<Integer, Integer> vxCount = datas.get(i);
                outStr.append("Top" + (i + 1) + "\n");
                outStr.append("vc = " + vxCount.f0 + "\n");
                outStr.append("couont = " + vxCount.f1);
                outStr.append("窗口结束事件" + DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS") + "\n");
                outStr.append("==========================");
            }
            out.collect(outStr.toString());
        }
    }
}
