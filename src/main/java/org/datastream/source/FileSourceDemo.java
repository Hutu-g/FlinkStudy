package org.datastream.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: hutu
 * @Date: 2024/7/31 15:35
 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource.forRecordStreamFormat
                        (
                            new TextLineInputFormat(),
                            new Path("input/word.txt")
                        )
                         .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"fileSourceDemo").print();
        env.execute();
    }
}
