package org.datastream.transform.splitwater;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.datastream.bean.WaterSensor;

/**
 * @Author: hutu
 * @Date: 2024/7/29 16:45
 */
public class SplitByOutputDemo {
    public static void main(String[] args) throws Exception {

        //创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据
        //将WaterSensor按照Id类型进行分流。s1侧s2侧，其他主
        DataStreamSource<WaterSensor> sensorDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 1L, 5),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3));

        //创建测流标签
        OutputTag<WaterSensor> s1Tag = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> s2Tag = new OutputTag<>("s2", Types.POJO(WaterSensor.class));
        //创建测分流
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String id = value.getId();
                if ("s1".equals(id)) {
                    ctx.output(s1Tag, value);
                } else if ("s2".equals(id)) {
                    ctx.output(s2Tag, value);
                } else {
                    out.collect(value);
                }
            }
        });
        //打印主流
        process.print("主流");
        process.getSideOutput(s1Tag).print("s1");
        process.getSideOutput(s2Tag).print("s2");
        env.execute();
    }
}
