package com.fang.spark

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by fang on 17-1-5.
  */
object ComputeHistogram {
  def main(args: Array[String]): Unit = {
    val beginComputeHistogram = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      //.setMaster("local[4]")
      //.setMaster("spark://fang-ubuntu:7077")
      .setAppName("ComputeHistogram")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val hbaseConf = HBaseConfiguration.create()
    //table name
    val tableName = "imagesTest"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)
    val readSiftTime = System.currentTimeMillis()
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    SparkUtils.printComputeTime(readSiftTime,"read sift")
    val transformSift = System.currentTimeMillis()
    val siftRDD = hbaseRDD.map{
      result =>
        val siftByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("sift"))
        val siftArray: Array[Float] = Utils.deserializeMat(siftByte)
        val size = siftArray.length / 128
        val siftTwoDim = new Array[Array[Float]](size)
        for (i <- 0 to size - 1) {
          val xs: Array[Float] = new Array[Float](128)
          for (j <- 0 to 127) {
            xs(j) = siftByte(i * 128 + j)
          }
          siftTwoDim(i) = xs
        }
        (result._2.getRow,siftTwoDim)
    }
    SparkUtils.printComputeTime(transformSift,"transform sift")
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val loadKmeanModelTime = System.currentTimeMillis()
    val myKmeansModel = KMeansModel.load(sc,SparkUtils.kmeansModelPath)
    SparkUtils.printComputeTime(loadKmeanModelTime,"load kmeans mode")
    val computeHistogram = System.currentTimeMillis()
    val histogramRDD = siftRDD.map {
      result => {
        /**一个Put对象就是一行记录，在构造方法中指定主键
         * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
         * Put.add方法接收三个参数：列族，列名，数据
         */
        val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
        for (i <- 0 to result._2.length - 1) {
          val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(result._2(i).map(i=>i.toDouble)))
          histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
        }
        val put: Put = new Put(result._1)
        put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("histogram"), Utils.serializeObject(histogramArray))
        (new ImmutableBytesWritable, put)
      }
    }
    SparkUtils.printComputeTime(computeHistogram,"compute histogram")
    val saveHistogram = System.currentTimeMillis()
    histogramRDD.saveAsHadoopDataset(jobConf)
    SparkUtils.printComputeTime(saveHistogram,"save histogram")
    SparkUtils.printComputeTime(beginComputeHistogram,"the compute histogram job")
  }
}
