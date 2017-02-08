package com.fang.spark.demo

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fang on 17-1-20.
  * 读取Hive中sell表的数据进行kmeans模型训练
  * 对测试数据进行预测,并将结果保存到HDFS中
  * 将预测结果在R中显示
  */
object HiveDataTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HiveDataTest1")
      .setMaster("spark://fang-ubuntu:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("SET spark.sql.shuffle.partitions=20")
    val sqldata = sqlContext.sql("select locationid, sum(num) allnum,sum(amount) allamount from sell group by locationid ")
    sqldata.collect().foreach(println)
    val parsedData = sqldata.map {
      case Row(_, allnum, allamount) =>
        val features = Array[Double](allnum.toString.toDouble, allamount.toString.toDouble)
        Vectors.dense(features)
    }
    parsedData.foreach(println)
    //对数据集聚类,3个类,20次迭代,形成数据模型 注意这里会使用设置的partition数20
    val numClusters = 3
    val numIterations = 20
    val model = KMeans.train(parsedData, numClusters, numIterations)
    //用模型对读入的数据进行分类,并显示
    val result1 = sqldata.map {
      case Row(locationid, allnum, allamount) =>
        val features = Array[Double](allnum.toString.toDouble, allamount.toString.toDouble)
        val linevectore = Vectors.dense(features)
        val prediction = model.predict(linevectore)
        locationid + " " + allnum + " " + allamount + " " + prediction
    }.foreach(println)
    //保存文件
    val result2 = sqldata.map {
      case Row(locationid, allnum , allamount) =>
        val features = Array[Double](allnum.toString.toDouble, allamount.toString.toDouble)
        val linevectore = Vectors.dense(features)
        val prediction = model.predict(linevectore)
        locationid + " " + allnum + " " + allamount + " " + prediction
    }.saveAsTextFile("/user/hadoop/sell")

  }

}
