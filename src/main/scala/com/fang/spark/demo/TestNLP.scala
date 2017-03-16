package com.fang.spark.demo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import functions._
/**
  * Created by fang on 17-3-15.
  */
object TestNLP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-corenlp-master").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val input = Seq(
      (1, "<xml>Stanford University is located in California. It is a great university.</xml>")
    ).toDF("id", "text")

    val output = input
      .select(cleanxml('text).as('doc))
      .select(explode(ssplit('doc)).as('sen))
     // .select('sen, tokenize('sen).as('words), ner('sen).as('nerTags), sentiment('sen).as('sentiment))

    output.show(truncate = false)
  }
}
