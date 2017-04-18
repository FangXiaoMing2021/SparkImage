package com.fang.spark

/**
  * Created by fang on 16-12-15.
  * 对存储在HBase中的image表中的数据进行KMeans聚类，并保存训练完成的模型
  */

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KMeansForHarrisInHBase extends App {
  val beginKMeans = System.currentTimeMillis()
  val sparkConf = ImagesUtil.loadSparkConf("KMeansForHarrisInHBase")
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("WARN")
  val readSiftTime = System.currentTimeMillis()
  val hbaseConf = ImagesUtil.loadHBaseConf()
  val tableName = ImagesUtil.imageTableName
  hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

  val scan = new Scan()
//  scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"))
  //添加harris
  scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("harris"))
  val proto = ProtobufUtil.toScan(scan)
  val ScanToString = Base64.encodeBytes(proto.toByteArray())
  hbaseConf.set(TableInputFormat.SCAN, ScanToString)
  val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    classOf[org.apache.hadoop.hbase.client.Result])
  ImagesUtil.printComputeTime(readSiftTime, "readHarrisTime")
  val transformSift = System.currentTimeMillis()
  val siftRDD = hbaseRDD.map {
    result =>
      val siftByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("harris"))
      //HBase中没有数据异常
      //val harrisByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("harris"))
      //空指针异常
      if(siftByte!=null){
        val featureTwoDim = Utils.ByteToTwoArrayHarris(siftByte)
        (result._2.getRow, featureTwoDim)
      }else{
        null
      }
  }.filter(_!=null)
  //siftRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
  val siftDenseRDD = siftRDD.flatMap(_._2)
    .map(data => Vectors.dense(data)).repartition(40).cache()
    //.persist(StorageLevel.MEMORY_AND_DISK)
  ImagesUtil.printComputeTime(transformSift, "tranform harris")
  val kmeansTime = System.currentTimeMillis()
  val numClusters = 300
  val numIterations = 30
  val runTimes = 3
  var clusterIndex: Int = 0
  // java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE
  val clusters: KMeansModel = KMeans.train(siftDenseRDD, numClusters, numIterations, runTimes)
  clusters.save(sc, ImagesUtil.kmeansModelPath)
  ImagesUtil.printComputeTime(kmeansTime, "kmeansTime")
  sc.stop()
  ImagesUtil.printComputeTime(beginKMeans, "time for the job")

}

