package com.fang.spark.bakeup

import com.fang.spark.demo.ImageInputFormat
import com.fang.spark.{ImagesUtil, Utils}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{Core, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}

/**
  * Created by hadoop on 16-11-15.
  * 批量上传数据到HBase表中，并计算sift值
  * 表结构为：
  * imagesTable：(image:imageName,imageBinary,feature:sift,columnDiagram)
  * RowKey设计为：
  * 该功能包括:读取本地图像文件集,计算每张图片的sift,上传二进制图像和sift特征值到Hbase中
  * 需要统计的时间包括,读取本地文件时间,计算sift时间,上传数据时间
  */

object HBaseUpLoadImagesHaveTune {
  def main(args: Array[String]): Unit = {
    val beginUpload = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .setAppName("HBaseUpLoadImages").
      setMaster("local[4]").
      // setMaster("spark://fang-ubuntu:7077").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    //统计获取本地数据文件的时间
    val begUpload = System.currentTimeMillis()
    val imagesRDD = sparkContext.newAPIHadoopFile[Text, Mat, ImageInputFormat]("file:///home/fang/images/train/2")
    ImagesUtil.printComputeTime(begUpload, "upload image")
    println(imagesRDD.getNumPartitions)
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    //统计连接HBase数据库的时间
    val begConnHBase = System.currentTimeMillis()
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu")
    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig)
    val tableName = "imagesTest"
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    ImagesUtil.printComputeTime(begConnHBase, "connect hbase")
    //统计计算sift时间
    val begComputeSift = System.currentTimeMillis()
    imagesRDD.foreach {
      //加载Opencv库,在每个分区都需加载
      imageFile => {
        val put: Put = new Put(Bytes.toBytes(imageFile._1.toString))
        val desc = new Mat
        val fd = FeatureDetector.create(FeatureDetector.SIFT)
        val mkp = new MatOfKeyPoint
        fd.detect(imageFile._2, mkp)
        val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
        //提取sift特征
        de.compute(imageFile._2, mkp, desc)
        if (imageFile._2.rows() != 0) {
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"), Bytes.toBytes(imageFile._2.dump()))
        }
        //put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("path"), Bytes.toBytes(imageFile._1))
        if (desc.rows() != 0) {
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"), Utils.serializeMat(desc))
        }
        table.put(put)
      }
    }
    ImagesUtil.printComputeTime(begComputeSift, "compute sift")
    connection.close()
    sparkContext.stop()
    ImagesUtil.printComputeTime(beginUpload, "程序运行总时间")
  }


}
