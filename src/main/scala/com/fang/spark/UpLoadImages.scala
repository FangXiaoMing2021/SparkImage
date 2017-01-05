package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{Core, CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}

/**
  * Created by hadoop on 17-01-05.
  * 批量上传数据到HBase表中，并计算sift值
  * 表结构为：
  * imagesTable：(image:imageName,imageBinary,imageClass,sift,histodiagram)
  * RowKey设计为：
  */

object UpLoadImages {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("UpLoadImages").
      //setMaster("local[4]").
      //setMaster("spark://fang-ubuntu:7077").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkContext = new SparkContext(sparkConf)
    val imagesRDD = sparkContext.binaryFiles("file:///home/hadoop/ILSVRC2015/Data/CLS-LOC/train/n02113799")
    //val imagesRDD = sparkContext.binaryFiles("file:///home/fang/images/train/1")
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConfig.set("hbase.zookeeper.quorum", "fang-ubuntu")
    val tableName = "imagesTest"
    val jobConf = new JobConf(hbaseConfig)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val imageSiftRDD = imagesRDD.map{
          imageFile => {
            System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
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
            (new ImmutableBytesWritable, put)
          }
    }
    imageSiftRDD.saveAsHadoopDataset(jobConf)
    sparkContext.stop()
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
