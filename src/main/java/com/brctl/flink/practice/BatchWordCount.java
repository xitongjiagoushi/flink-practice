package com.brctl.flink.practice;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

/**
 * Batch Word Count<br/>
 * use <code>mvn compile exec:java -Dexec.mainClass=com.brctl.flink.practice.BatchWordCount</code> to run the example
 * @author duanxiaoxing
 * @created 2018/1/4
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> inputText = environment.fromElements(
                "hello world",
                "hello world",
                "hello"
        ).setParallelism(1);

        @SuppressWarnings("unchecked")
        // checked type convert
        DataSet<Tuple2<String, Integer>> counts = inputText.flatMap((text, collector) -> {
            String[] words = text.split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }

        })
        .returns(new TupleTypeInfo(TupleTypeInfo.of(String.class), TupleTypeInfo.of(Integer.class)))  // adapt to Java 8
        .groupBy(0)
        .sum(1)
        .setParallelism(1);

        // execute and print result
        counts.print();
    }

}
