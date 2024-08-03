package org.datastream.bean;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * 我们可以单独定义一个函数类实现ReduceFunction接口，也可以直接传入一个匿名类。当然，同样也可以通过传入Lambda表达式实现类似的功能。
 * 为了方便后续使用，定义一个WaterSensorMapFunction：
 * @Author: hutu
 * @Date: 2024/8/2 17:34
 */
public class WaterSensorMapFunction implements MapFunction<String,WaterSensor> {
    @Override
    public WaterSensor map(String value) throws Exception {
        String[] datas = value.split(",");
        return new WaterSensor(datas[0],Long.valueOf(datas[1]) ,Integer.valueOf(datas[2]) );
    }
}
