package com.fang.spark.StreamingAndHBase;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

//import org.apache.spark.examples.streaming.StreamingExamples;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * 
 * Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads> <zkQuorum>
 * is a list of one or more zookeeper servers that make quorum <group> is the
 * name of kafka consumer group <topics> is a list of one or more kafka topics
 * to consume from <numThreads> is the number of threads the kafka consumer
 * should use
 * 
 * To run this example: `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \ zoo03
 * my-consumer-group topic1,topic2 1`
 */
/*
 * /root/spark-1.2.0-bin-hadoop2.3/bin/spark-submit --class
 * org.apache.spark.examples.streaming.JavaKafkaWordCount --master local[8]
 * /root/testhbase-0.0.1-SNAPSHOT.jar 100
 */
public final class JavaKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  private JavaKafkaWordCount() {
  }

  public static void main(String[] args) {
    // if (args.length < 4) {
    // System.err
    // .println("Usage: JavaKafkaWordCount <zkQuorum> <group> <topics> <numThreads>");
    // System.exit(1);
    // }

    String zkQuorum = "localhost";
    String group = "test-consumer-group";
    String topicss = "test";
    String numThread = "2";

    // StreamingExamples.setStreamingLogLevels();
    SparkConf sparkConf = new SparkConf().setAppName("JavaKafkaWordCount");
    // Create the context with a 1 second batch size
    JavaStreamingContext jssc =
        new JavaStreamingContext(sparkConf, new Duration(2000));

    int numThreads = Integer.parseInt(numThread);
    Map<String, Integer> topicMap = new HashMap<String, Integer>();
    String[] topics = topicss.split(",");
    for (String topic : topics) {
      topicMap.put(topic, numThreads);
    }

    JavaPairReceiverInputDStream<String, String> messages =
        KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

    JavaDStream<String> lines =
        messages.map(new Function<Tuple2<String, String>, String>() {
         // @Override
          public String call(Tuple2<String, String> tuple2) {
            return tuple2._2();
          }
        });

    JavaDStream<String> words =
        lines.flatMap(new FlatMapFunction<String, String>() {
        //  @Override
          public Iterable<String> call(String x) {
            return Lists.newArrayList(SPACE.split(x));
          }
        });

    JavaPairDStream<String, Integer> wordCounts =
        words.mapToPair(new PairFunction<String, String, Integer>() {
         // @Override
          public Tuple2<String, Integer> call(String s) {
            return new Tuple2<String, Integer>(s, 1);
          }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
          //@Override
          public Integer call(Integer i1, Integer i2) {
            return i1 + i2;
          }
        });

    wordCounts.print();
    jssc.start();
    jssc.awaitTermination();
  }
}
