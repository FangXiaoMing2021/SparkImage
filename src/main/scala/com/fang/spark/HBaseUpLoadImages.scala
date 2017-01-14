package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{Core, CvType, Mat, MatOfKeyPoint}
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

object HBaseUpLoadImages {
  def main(args: Array[String]): Unit = {
    val beginUpload = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .setAppName("HBaseUpLoadImages").
      //setMaster("local[4]").
      setMaster("spark://fang-ubuntu:7077").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    //统计获取本地数据文件的时间
    val begUpload = System.currentTimeMillis()
    //val imagesRDD = sparkContext.binaryFiles("file:///home/hadoop/ILSVRC2015/Data/CLS-LOC/train/n02113799")
    val imagesRDD = sparkContext.binaryFiles(SparkUtils.imagePath)
    SparkUtils.printComputeTime(begUpload, "upload image")
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
        val tableName = "imagesTest"
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        SparkUtils.printComputeTime(begConnHBase, "connect hbase")
        //统计计算sift时间
        val begComputeSift = System.currentTimeMillis()
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
            val sift = getImageSift(imageBinary)
            if (!sift.isEmpty) {
              put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"), sift.get)
            }
            table.put(put)
          }
        }
        SparkUtils.printComputeTime(begComputeSift, "compute sift")
        connection.close()
      }
    }
    sparkContext.stop()
    SparkUtils.printComputeTime(beginUpload,"程序运行总时间")
  }

  //获取图像的sift特征
  def getImageSift(image: Array[Byte]): Option[Array[Byte]] = {
    val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
    val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    test_mat.put(0, 0, data)
    val desc = new Mat
    val fd = FeatureDetector.create(FeatureDetector.SIFT)
    val mkp = new MatOfKeyPoint
    fd.detect(test_mat, mkp)
    val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
    //提取sift特征
    de.compute(test_mat, mkp, desc)
    test_mat.release()
    mkp.release()
    //判断是否有特征值
    if (desc.rows() != 0) {
      Some(Utils.serializeMat(desc))
    } else {
      None
    }
  }

}
