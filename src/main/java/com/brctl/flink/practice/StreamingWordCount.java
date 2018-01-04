package com.brctl.flink.practice;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Streaming Word Count<br/>
 * use <code>mvn compile exec:java -Dexec.mainClass=com.brctl.flink.practice.StreamingWordCount</code> to run the example
 * @author duanxiaoxing
 * @created 2018/1/4
 */
public class StreamingWordCount {

    public static void main(String[] args) throws Exception {
        String jobName = "Streaming Word Count";
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = environment.fromElements(
                "hello world",
                "hello world",
                "hello"
        ).setParallelism(1);

        @SuppressWarnings("unchecked")
        // checked type convert
        DataStream<Tuple2<String, Integer>> counts = text.flatMap((line, collector) -> {
            String[] words = line.split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        })
        .returns(new TupleTypeInfo(TupleTypeInfo.of(String.class), TupleTypeInfo.of(Integer.class)))
        .keyBy(0)
        .sum(1)
        .setParallelism(1);

        counts.print().setParallelism(1);

        environment.execute(jobName);
    }

}
