package com.fang.spark

/**
  * Created by fang on 16-12-15.
  */

import java.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object HbaseRDD extends App {

  val sparkConf = new SparkConf().setMaster("local[4]")
    .setAppName("My App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  var hbaseConf = HBaseConfiguration.create()
  hbaseConf.set(TableInputFormat.INPUT_TABLE, "imagesTest")
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
  var scan = new Scan()
  scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"))
  var proto = ProtobufUtil.toScan(scan)
  var ScanToString = Base64.encodeBytes(proto.toByteArray())
  hbaseConf.set(TableInputFormat.SCAN, ScanToString)
  val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  val siftRDD = hbaseRDD.map(x => x._2)
    .flatMap {
      result =>
        val siftByte = result.getValue(Bytes.toBytes("image"), Bytes.toBytes("sift"))
        val siftArray: Array[Double] = Utils.deserializeMat(siftByte)
        val size = siftArray.length / 128
        val siftTwoDim = new Array[Array[Double]](size)
        for (i <- 0 until size) {
          val xs: Array[Double] = new Array[Double](128)
          siftArray.copyToArray(xs, i * 128, 128)
          siftTwoDim(i) = xs
        }
        siftTwoDim
    }.map(data=>Vectors.dense(data))
  val numClusters = 8
  val numIterations = 30
  val runTimes = 3
  var clusterIndex: Int = 0
  val clusters: KMeansModel = KMeans.train(siftRDD, numClusters, numIterations, runTimes)
  println("Cluster Number:" + clusters.clusterCenters.length)
  println("Cluster Centers Information Overview:")
  clusters.save(sc,"src/main/resources/model.txt")
  clusters.clusterCenters.foreach(
    x => {
      println("Center Point of Cluster " + clusterIndex + ":")
      println(x)
      clusterIndex += 1
    })
  //begin to check which cluster each test data belongs to based on the clustering result
  val rawTestData = sc.textFile("src/main/resources/customerTest.txt")
  val parsedTestData = rawTestData.map(line => {
    Vectors.dense(line.split(",").map(_.trim).filter(!"".equals(_)).map(_.toDouble))
  })
  parsedTestData.collect().foreach(testDataLine => {
    val predictedClusterIndex: Int = clusters.predict(testDataLine)
    println("The data " + testDataLine.toString + " belongs to cluster " + predictedClusterIndex)
  })
  println("Spark MLlib K-means clustering test finished.")

}