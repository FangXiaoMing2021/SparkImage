package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{CvType, Mat, MatOfKeyPoint}
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
    val sparkConf = new SparkConf().setAppName("HBaseUpLoadImages").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val imagesRDD = sparkContext.binaryFiles("/home/fang/images/")
    // val imagesRDD = sparkContext.newAPIHadoopFile[Text, BufferedImage, ImmutableBytesWritable]("/home/fang/images/train/1")
    // val columnFaminlys :Array[String] = Array("image")
    //createTable(tableName,columnFaminlys,connection)
    imagesRDD.foreachPartition {
      iter => {
        val hbaseConfig = HBaseConfiguration.create()
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
        val connection: Connection = ConnectionFactory.createConnection(hbaseConfig);
        val tableName = "imagesTest"
        val table: Table = connection.getTable(TableName.valueOf(tableName))
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
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"), getImageSift(imageBinary))
            table.put(put)
          }
        }
        connection.close()
      }
    }

    sparkContext.stop()
  }

  //获取图像的sift特征
  def getImageSift(image: Array[Byte]): Array[Byte] = {
    val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(image))
    val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8U)
    //java.lang.UnsupportedOperationException:
    // Provided data element number (188000) should be multiple of the Mat channels count (3)
    //更改CvType.CV_8UC3为CvType.CV_8U,解决上面错误
    // val test_mat = new Mat(bi.getHeight, bi.getWidth, CvType.CV_8UC3)
    val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    // if(bi.getColorModel.getNumComponents==3) {
    test_mat.put(0, 0, data)
    val desc = new Mat
    val fd = FeatureDetector.create(FeatureDetector.SIFT)
    val mkp = new MatOfKeyPoint
    fd.detect(test_mat, mkp)
    val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
    //提取sift特征
    de.compute(test_mat, mkp, desc)
    Utils.serializeMat(desc)
  }

}
