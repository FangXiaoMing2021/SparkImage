package com.fang.spark

/**
  * Created by fang on 16-12-15.
  * 对存储在HBase中的image表中的数据进行KMeans聚类，并保存训练完成的模型
  */

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KMeansForSiftInHBase extends App {
  val beginKMeans = System.currentTimeMillis()
  val sparkConf = new SparkConf()
    //.setMaster("spark://fang-ubuntu:7077")
    .setMaster("local[4]")
    .setAppName("KMeansForSiftInHBase")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  val readSiftTime = System.currentTimeMillis()
  val hbaseConf = HBaseConfiguration.create()
  val tableName = SparkUtils.imageTableName
  hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
  val scan = new Scan()
  scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"))
  //添加harris
  scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("harris"))
  val proto = ProtobufUtil.toScan(scan)
  val ScanToString = Base64.encodeBytes(proto.toByteArray())
  hbaseConf.set(TableInputFormat.SCAN, ScanToString)
  val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  SparkUtils.printComputeTime(readSiftTime, "readSiftTime")
  val transformSift = System.currentTimeMillis()
  val siftRDD = hbaseRDD.map {
    result =>
      val siftByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("sift"))
      val harrisByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("harris"))
      val featureTwoDim = SparkUtils.featureArr2TowDim(siftByte,harrisByte)
      (result._2.getRow, featureTwoDim)
  }
  //siftRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  val siftDenseRDD = siftRDD.flatMap(_._2)
    .map(data => Vectors.dense(data.map(i => i.toDouble))).cache()
    //.persist(StorageLevel.MEMORY_AND_DISK)
  SparkUtils.printComputeTime(transformSift, "tranform sift")
  val kmeansTime = System.currentTimeMillis()
  val numClusters = 100
  val numIterations = 30
  val runTimes = 3
  var clusterIndex: Int = 0
  // java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE
  val clusters: KMeansModel = KMeans.train(siftDenseRDD, numClusters, numIterations, runTimes)
  clusters.save(sc, SparkUtils.kmeansModelPath)
  SparkUtils.printComputeTime(kmeansTime, "kmeansTime")
  sc.stop()
  SparkUtils.printComputeTime(beginKMeans, "time for the job")

}

