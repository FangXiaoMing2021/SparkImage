package com.fang.spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Created by fang on 16-12-4.
  */
object StreamingParseLogs {
  def main(args:Array[String]): Unit ={
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[4]").setAppName("StreamingWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("localhost", 8888)
    // Split each line into words
    val ip =lines.map{
      line =>{
        line.split(" ")(0)
      }}
    val page =lines.map{
      line =>{
        line.split(" ")(10)
      }}
    val agent =lines.map{
      line =>{
        val splits = line.split(" ")
        splits(splits.length-1)
      }}
    // Count each word in each batch
    val ipPairs = ip.map(word =>(word, 1))
    val ipCounts = ipPairs.reduceByKey(_+_).transform{
      rdd=>{
          rdd.map(tuple=>(tuple._2,tuple._1)).sortByKey().map(tuple=>(tuple._1,tuple._2))
       }
      }
    val pagePairs = page.map(word => (word, 1))
    val pageCounts = pagePairs.reduceByKey(_+_).transform{
      rdd=>{
         rdd.map(tuple=>(tuple._2,tuple._1)).sortByKey().map(tuple=>(tuple._1,tuple._2))
      }
    }
    val agentPairs = agent.map(word => (word, 1))
    val agentCounts = agentPairs.reduceByKey(_+_)
    // Print the first ten elements of each RDD generated in this DStream to the console
    ipCounts.saveAsTextFiles("/home/fang/Document/ipCounts")
    pageCounts.saveAsTextFiles("/home/fang/Document/pageCounts")
    agentCounts.saveAsTextFiles("/home/fang/Document/agentCounts")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
