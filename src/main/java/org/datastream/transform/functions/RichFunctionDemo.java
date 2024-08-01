package org.datastream.transform.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hutu
 * @Date: 2024/8/1 10:12
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4);
        SingleOutputStreamOperator<Integer> map = source.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value + 1;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("open"+"子任务编号"+ getRuntimeContext().getIndexOfThisSubtask() + "子任务名称"+ getRuntimeContext().getTaskNameWithSubtasks());
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("close"+"子任务编号"+ getRuntimeContext().getIndexOfThisSubtask() + "子任务名称"+ getRuntimeContext().getTaskNameWithSubtasks());

            }
        });
        map.print();
        env.execute();
    }
}
