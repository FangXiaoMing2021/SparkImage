package com.fang.spark.demo

import java.util.HashMap

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

import scala.io.Source

object KafkaLogsProcess {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaLogsProcess <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaLogsProcess")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val ip = lines.map {
      line => {
        line.split(" ")(0)
      }
    }
    val page = lines.map {
      line => {
        line.split(" ")(10)
      }
    }
    val agent = lines.map {
      line => {
        val splits = line.split(" ")
        splits(splits.length - 1)
      }
    }
    // Count each word in each batch
    val ipPairs = ip.map(word => (word, 1))
    val ipCounts = ipPairs.reduceByKey(_ + _).transform {
      rdd => {
        rdd.map(tuple => (tuple._2, tuple._1)).sortByKey().map(tuple => (tuple._1, tuple._2))
      }
    }
    val pagePairs = page.map(word => (word, 1))
    val pageCounts = pagePairs.reduceByKey(_ + _).transform {
      rdd => {
        rdd.map(tuple => (tuple._2, tuple._1)).sortByKey().map(tuple => (tuple._1, tuple._2))
      }
    }
    val agentPairs = agent.map(word => (word, 1))
    val agentCounts = agentPairs.reduceByKey(_ + _)
    ssc.start()
    ssc.awaitTermination()
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "imagesTest")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val topServerTable = connection.getTable(TableName.valueOf("topServerTable"))
    pageCounts.foreachRDD {
      iter => iter.foreach{
       tuple => {
          val topServerPut: Put = new Put(Bytes.toBytes(tuple._1))
          topServerPut.addColumn(Bytes.toBytes("page"), Bytes.toBytes("cf1"), Bytes.toBytes(tuple._2))
          topServerTable.put(topServerPut)
        }
      }
    }

    val customerAreaTable = connection.getTable(TableName.valueOf("customerAreaTable"))
    ipCounts.foreachRDD {
      iter =>
        iter.foreach {
          tuple => {
            val customerAreaPut: Put = new Put(Bytes.toBytes(tuple._1))
            customerAreaPut.addColumn(Bytes.toBytes("ipCount"), Bytes.toBytes("cf1"), Bytes.toBytes(tuple._2))
            customerAreaTable.put(customerAreaPut)
          }
        }
    }
    val pvTable = connection.getTable(TableName.valueOf("pvTable"))
    agentCounts.foreachRDD {
      iter =>
        iter.foreach {
          tuple => {
            val pvPut: Put = new Put(Bytes.toBytes(tuple._1))
            pvPut.addColumn(Bytes.toBytes("pv"), Bytes.toBytes("cf1"), Bytes.toBytes(tuple._2))
            pvTable.put(pvPut)
          }
        }
    }
    ssc.stop()
  }
}


object KafkaFileProducer {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaFileProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    // Zookeeper connection properties
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val file = Source.fromFile("/home/fang/Downloads/apache_log.txt")
    for (line <- file.getLines()) {
      val message = new ProducerRecord[String, String](topic, null, line)
      producer.send(message)
      Thread.sleep(100)
    }
  }

}
