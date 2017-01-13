package com.fang.spark.bakeup

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import com.fang.spark.{SparkUtils, Utils}
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
  * imagesTable：(image:imageName,imageBinary,imageClass,source),(feature:sift,columnDiagram)
  * RowKey设计为：
  */

object HBaseUpLoadImages {
  def main(args: Array[String]): Unit = {
    //System.setProperty("java.library.path","/home/fang/BigDataSoft/opencv-2.4.13/release/lib")
    val sparkConf = new SparkConf()
      .setAppName("HBaseUpLoadImages").
      //setMaster("local[4]").
      //setMaster("spark://fang-ubuntu:7077").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    //TODO 单机测试情况下，图片文件太多，程序运行失败，打出hs_err_pid***_log日志，具体情况不明
    //Error in `/usr/lib/jvm/jdk1.8.0_77/bin/java': malloc(): memory corruption: 0x00007fef7517d760 ***
    //train/1 出错
    //train/2 正常
    //train/3 出错,打印日志 Failed to write core dump. Core dumps have been disabled. To enable core dumping, try "ulimit -c unlimited" before starting Java again
    //内存不够时出现溢出
    //先运行ulimit -c unlimited
    //统计获取本地数据文件的时间
    val begUpload = System.currentTimeMillis()
    val imagesRDD = sparkContext.binaryFiles("file:///home/hadoop/ILSVRC2015/Data/CLS-LOC/train/n02113799")
    SparkUtils.printComputeTime(begUpload,"upload")
    // val imagesRDD = sparkContext.newAPIHadoopFile[Text, BufferedImage, ImmutableBytesWritable]("/home/fang/images/train/1")
    // val columnFaminlys :Array[String] = Array("image")
    //createTable(tableName,columnFaminlys,connection)
    imagesRDD.foreachPartition {
      iter => {
        //java.lang.UnsatisfiedLinkError: org.opencv.core.Mat.n_Mat(III)J
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
        //统计连接HBase数据库的时间
        val begConnHBase = System.currentTimeMillis()
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu")
        val connection: Connection = ConnectionFactory.createConnection(hbaseConfig);
        val tableName = "imagesTest"
        val table: Table = connection.getTable(TableName.valueOf(tableName))
        SparkUtils.printComputeTime(begConnHBase,"connect hbase")
        iter.foreach {
          imageFile => {
            //val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(imageFile._2.toArray()))
            //println(bi.getColorModel)
            val tempPath = imageFile._1.split("/")
            val len = tempPath.length
            val imageName = tempPath(len - 1)
            //TODO 尝试直接获取BufferedImage数据，提升效率
            val imageBinary: scala.Array[Byte] = imageFile._2.toArray()
            val put: Put = new Put(Bytes.toBytes(imageName))
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"), imageBinary)
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("path"), Bytes.toBytes(imageFile._1))
            //统计计算sift时间
            val begComputeSift = System.currentTimeMillis()
            val sift = getImageSift(imageBinary)
            SparkUtils.printComputeTime(begComputeSift,"compute sift")
            if(!sift.isEmpty) {
              put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"),sift.get)
            }
              table.put(put)
          }
        }
        connection.close()
      }
    }
    sparkContext.stop()
  }

  //获取图像的sift特征
  def getImageSift(image: Array[Byte]):Option[Array[Byte]] = {
    val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
    val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    //java.lang.UnsupportedOperationException:
    // Provided data element number (188000) should be multiple of the Mat channels count (3)
    //更改CvType.CV_8UC3为CvType.CV_8U,解决上面错误
    // val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
//    if (bi.getColorModel.getNumComponents == 3) {
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
      if (desc.rows()!= 0) {
        Some(Utils.serializeMat(desc))
      } else {
        None
      }
  }

}
