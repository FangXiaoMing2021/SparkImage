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
import org.opencv.core.Core

/**
  * Created by fang on 17-1-5.
  */
object SaveHarrisInHBase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[4]")
      //.setMaster("spark://fang-ubuntu:7077")
      .setAppName("SaveHarrisInHBase")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val hbaseConf = HBaseConfiguration.create()
    //table name
    val tableName = SparkUtils.imageTableName
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)
    val readSiftTime = System.currentTimeMillis()
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    SparkUtils.printComputeTime(readSiftTime, "read image")
    val transformHarris = System.currentTimeMillis()
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val histogramRDD = hbaseRDD.map {
      result => {
        //提取HARRIS特征
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
        //hbase java.lang.IllegalArgumentException: No columns to insert
        //Put中没值
        var tuple : (ImmutableBytesWritable, Put) =null
        val imageByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("binary"))
        val harris = SparkUtils.getImageHARRIS(imageByte)
        if (!harris.isEmpty) {
          val put: Put = new Put(result._2.getRow)
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("harris"), harris.get)
          tuple=(new ImmutableBytesWritable, put)
        }else{
          println(Bytes.toString(result._2.getRow))
        }
       tuple
      }
    }.filter(_!=null)
    println(histogramRDD.count())

    SparkUtils.printComputeTime(transformHarris, "transformHarris")
    val saveHarris = System.currentTimeMillis()

    histogramRDD.saveAsHadoopDataset(jobConf)
    SparkUtils.printComputeTime(saveHarris, "save harris")
  }
}
