package com.fang.spark

import java.net.InetAddress
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.Core

/**
  * Created by hadoop on 16-11-15.
  * 批量上传数据到HBase表中，并计算sift值
  * 表结构为：
  * imagesTable：(image:imageName,imageBinary,feature:sift,columnDiagram)
  * RowKey设计为：
  * 该功能包括:读取本地图像文件集,计算每张图片的sift,上传二进制图像和sift特征值到Hbase中
  * 需要统计的时间包括,读取本地文件时间,计算sift时间,上传数据时间
  */

object HBaseUpLoadImages {
  def main(args: Array[String]): Unit = {
    val beginUpload = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .setAppName("HBaseUpLoadImages")
      //.setMaster("local[2]")
      //.setMaster("spark://fang-ubuntu:7077")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    //统计获取本地数据文件的时间
    val begUpload = System.currentTimeMillis()
    val imagesRDD = sparkContext.binaryFiles(ImagesUtil.imagePath)
    ImagesUtil.printComputeTime(begUpload, "upload image")
    println("num of partition " + imagesRDD.getNumPartitions)
    imagesRDD.foreachPartition {
      iter => {
        //加载Opencv库,在每个分区都需加载
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
        //统计连接HBase数据库的时间
        val begConnHBase = System.currentTimeMillis()
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
        val connection: Connection = ConnectionFactory.createConnection(hbaseConfig);
        val tableName = ImagesUtil.imageTableName
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        ImagesUtil.printComputeTime(begConnHBase, "connect hbase")
        //统计计算sift时间
        val begComputeSift = System.currentTimeMillis()
        val host = InetAddress.getLocalHost()
        iter.foreach {
          imageFile => {
            val tempPath = imageFile._1.split("/")
            val len = tempPath.length
            val imageName = tempPath(len - 1)
            //TODO 尝试直接获取BufferedImage数据，提升效率
            val imageBinary: scala.Array[Byte] = imageFile._2.toArray()
            val put: Put = new Put(Bytes.toBytes(imageName))
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"), imageBinary)
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("path"), Bytes.toBytes(imageFile._1))
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("hostname"), Bytes.toBytes(host.getHostAddress + host.getHostName))
            val sift = ImagesUtil.getImageSift(imageBinary)
            if (!sift.isEmpty) {
              put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"), sift.get)
            }
            table.put(put)
          }
        }
        ImagesUtil.printComputeTime(begComputeSift, "compute sift")
        connection.close()
      }
    }
    sparkContext.stop()
    ImagesUtil.printComputeTime(beginUpload, "程序运行总时间")
  }

}
