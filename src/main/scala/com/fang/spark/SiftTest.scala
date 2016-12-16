package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{Core, CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}

/**
  * Created by fang on 16-12-12.
  */
object SiftTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseUpLoadImages").setMaster("local[4]")
    val sparkContext = new SparkContext(sparkConf)
    val imagesRDD = sparkContext.binaryFiles("/home/fang/images/")
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    imagesRDD.foreach {
      image => {
        val portable:PortableDataStream= image._2
        val arr:Array[Byte] = portable.toArray()
        val bi:BufferedImage= ImageIO.read(new ByteArrayInputStream(arr))
        //ExtractSift.sift(portable)
        val test_mat = new Mat(bi.getHeight, bi.getWidth,CvType.CV_8U)
        val data = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
        test_mat.put(0, 0, data)
        val desc = new Mat
        val fd = FeatureDetector.create(FeatureDetector.SIFT)
        val mkp = new MatOfKeyPoint
        fd.detect(test_mat, mkp)
        val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
        de.compute(test_mat, mkp, desc) //提取sift特征
//        println(desc.row(0).size())
//        println( desc.row(0).dump())
//        println(desc.total())

        println("-------------------")
        println(bi.getColorModel.getNumComponents)
        println(image._1)
        println(desc.rows())
        println(desc.cols())
        Utils.serializeMat(desc)
        //println(desc.dump())
        //val mat :Mat= new Mat(img.getHeight(),img.getWidth(), CvType.CV_8UC3);
      }
    }
  }
}
