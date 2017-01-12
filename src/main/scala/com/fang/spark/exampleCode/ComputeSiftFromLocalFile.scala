package com.fang.spark.exampleCode

import java.awt.Image
import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.{ByteArrayInputStream, File}
import javax.imageio.ImageIO

import com.fang.spark.{ImageInputFormat, Utils}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.{FileInputFormat, TextInputFormat}
import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import org.opencv.core.{Core, CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}

/**
  * Created by fang on 16-12-12.
  * 计算本地图像数据的sift特征值
  */
object ComputeSiftFromLocalFile {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseUpLoadImages").setMaster("local[3]")
    val sparkContext = new SparkContext(sparkConf)
    //val imagesRDD = sparkContext.binaryFiles("/home/fang/images/train/1")
    val imagesRDD = sparkContext.newAPIHadoopFile[Text, Mat, ImageInputFormat]("/home/fang/images/test")
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    imagesRDD.foreach {
          image => {

//            val portable: PortableDataStream = image._2
//            val arr: Array[Byte] = portable.toArray()
//            val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(arr))
            //val bi = image._2
            //ExtractSift.sift(portable)
            val test_mat = image._2
            val desc = new Mat
            val fd = FeatureDetector.create(FeatureDetector.SIFT)
            val mkp = new MatOfKeyPoint
            fd.detect(test_mat, mkp)
            val de = DescriptorExtractor.create(DescriptorExtractor.SIFT)
            de.compute(test_mat, mkp, desc) //提取sift特征
//            test_mat.release()
//            mkp.release()
//            desc.release()
            //println(desc.row(0).size())
            //println( desc.row(0).dump())
            //println(desc.total())
            //println("-------------------")
            //println(bi.getColorModel.getNumComponents)
            //println(image._1)
            println(image._1)
            println(desc.rows())
            println(desc.cols())
            //Utils.serializeMat(desc)
            //println(desc.dump())
            //val mat :Mat= new Mat(img.getHeight(),img.getWidth(), CvType.CV_8UC3);
          }
        }
  }
}
