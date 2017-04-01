package com.fang.spark

import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by fang on 17-1-5.
  */
object ComputeHarrisHistogram {
  def main(args: Array[String]): Unit = {
    val beginComputeHistogram = System.currentTimeMillis()
    val sparkConf = ImagesUtil.loadSparkConf("ComputeHarrisHistogram")

    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    val hbaseConf =ImagesUtil.loadHBaseConf()
    //table name
    val tableName = ImagesUtil.imageTableName
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()
    //scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"))
    scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("harris"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)
    val readSiftTime = System.currentTimeMillis()
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    ImagesUtil.printComputeTime(readSiftTime, "read sift")
    val transformSift = System.currentTimeMillis()
    val siftRDD = hbaseRDD.map {
      result => {
        val siftByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("harris"))
        // val siftTwoDim = siftArr2TowDim(siftByte)
        //(result._2.getRow, siftTwoDim)
        //val harrisByte = result._2.getValue(Bytes.toBytes("image"), Bytes.toBytes("harris"))
        var featureTwoDim:Array[Array[Float]] = null
        if(siftByte!=null){
          featureTwoDim = ImagesUtil.siftArr2TowDim(siftByte)
        }
        (result._2.getRow, featureTwoDim)
      }
    }.filter{tuple=>tuple._2!=null}

    ImagesUtil.printComputeTime(transformSift, "transform harris")
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val loadKmeanModelTime = System.currentTimeMillis()
    val myKmeansModel = KMeansModel.load(sc, ImagesUtil.kmeansModelPath)
    ImagesUtil.printComputeTime(loadKmeanModelTime, "load kmeans mode")
    val computeHistogram = System.currentTimeMillis()
    val histogramRDD = siftRDD.map {
      result => {
        /** 一个Put对象就是一行记录，在构造方法中指定主键
          * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
          * Put.add方法接收三个参数：列族，列名，数据
          */
        val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
        for (i <- 0 to result._2.length - 1) {
          val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(result._2(i).map(i => i.toDouble)))
          histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
        }
        val put: Put = new Put(result._1)
        //TODO 改了序列化
        put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("histogram"), ImagesUtil.ObjectToBytes(histogramArray))
        (new ImmutableBytesWritable, put)
      }
    }
    ImagesUtil.printComputeTime(computeHistogram, "compute histogram")
    val saveHistogram = System.currentTimeMillis()
    histogramRDD.saveAsHadoopDataset(jobConf)
    ImagesUtil.printComputeTime(saveHistogram, "save histogram")
    ImagesUtil.printComputeTime(beginComputeHistogram, "the compute histogram job")
  }
}
