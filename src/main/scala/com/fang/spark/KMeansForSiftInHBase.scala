package com.fang.spark

/**
  * Created by fang on 16-12-15.
  * 对存储在HBase中的image表中的数据进行KMeans聚类，并保存训练完成的模型
  */

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KMeansForSiftInHBase extends App {

  val sparkConf = new SparkConf().setMaster("local[4]")
    .setAppName("My App")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(sparkConf)
  var hbaseConf = HBaseConfiguration.create()
  val tableName= "imagesTest"
  hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
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
  //缓存hbaseRDD
  hbaseRDD.persist()
  val siftRDD = hbaseRDD.map(x => x._2)
    .flatMap {
      result =>
        val siftByte = result.getValue(Bytes.toBytes("image"), Bytes.toBytes("sift"))
        val siftArray: Array[Float] = Utils.deserializeMat(siftByte)
        //println(siftArray.length)
        val size = siftArray.length / 128
        //println(size)
        val siftTwoDim = new Array[Array[Float]](size)
        for (i <- 0 to size - 1) {
          val xs: Array[Float] = new Array[Float](128)
          //          println(i*128)
          //          siftArray.copyToArray(xs, i*128, 128)
          //          siftTwoDim(i) = xs
          for (j <- 0 to 127) {
            xs(j) = siftByte(i * 128 + j)
          }
          siftTwoDim(i) = xs
        }
        siftTwoDim
    }.map(data => Vectors.dense(data.map(i => i.toDouble)))
  val numClusters = 4
  val numIterations = 30
  val runTimes = 3
  var clusterIndex: Int = 0
  val clusters: KMeansModel = KMeans.train(siftRDD, numClusters, numIterations, runTimes)
  println("Cluster Number:" + clusters.clusterCenters.length)
  println("Cluster Centers Information Overview:")
  clusters.save(sc, "src/main/resources/KMeansModel")
  clusters.clusterCenters.foreach(
    x => {
      println("Center Point of Cluster " + clusterIndex + ":")
      println(x)
      clusterIndex += 1
    })
  val histogramRDD = hbaseRDD.foreachPartition {
    iter => {
      val connection: Connection = ConnectionFactory.createConnection(hbaseConf)
      val table: Table = connection.getTable(TableName.valueOf(tableName))
      iter.foreach {
        result =>
         // val rowKey = Bytes.toString(result._2.getRow())
          val histogramArray = new Array[Int](numClusters)
          val siftByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("sift"))
          val siftArray: Array[Float] = Utils.deserializeMat(siftByte)
          val size = siftArray.length / 128
          for (i <- 0 to size - 1) {
            val xs: Array[Float] = new Array[Float](128)
            for (j <- 0 to 127) {
              xs(j) = siftByte(i * 128 + j)
            }
            val predictedClusterIndex: Int = clusters.predict(Vectors.dense(xs.map(x => x.toDouble)))
            histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
          }
          val put: Put = new Put(result._2.getRow())
          put.addColumn(Bytes.toBytes("feature"), Bytes.toBytes("sift"), Utils.serializeObject(histogramArray))
      }

    }
  }


  }