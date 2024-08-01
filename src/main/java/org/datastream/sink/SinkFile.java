package org.datastream.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.compactor.SimpleStringDecoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

/**
 * @Author: hutu
 * @Date: 2024/8/1 20:24
 */
public class SinkFile {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //必须开启，否则不可查看
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "Number:" + value;
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(10), Types.STRING
        );

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("output/tmp"), new SimpleStringEncoder<>("UTF-8"))
                //输出文件配置，文件名前缀与后缀
                .withOutputFileConfig(
                        OutputFileConfig
                                .builder()
                                .withPartPrefix("hututu")
                                .withPartSuffix(".log")
                                .build()
                )
                //按目录分桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH"))
                //文件滚动策略
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(new MemorySize(1024))
                                .build()
                ).build();
        dataGen.sinkTo(fileSink);
        env.execute();
    }
}
