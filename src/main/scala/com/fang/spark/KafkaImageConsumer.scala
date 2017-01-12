package com.fang.spark

import java.awt.image.{BufferedImage, DataBufferByte}
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.opencv.core.{CvType, Mat, MatOfKeyPoint}
import org.opencv.features2d.{DescriptorExtractor, FeatureDetector}

/**
  * Created by fang on 16-12-21.
  */
object KafkaImageConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaImageProcess").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topics = Set("image_topic")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
      //"serializer.class" -> "kafka.serializer.DefaultDecoder",
      //"key.serializer.class" -> "kafka.serializer.StringEncoder"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    kafkaStream.foreachRDD {
      rdd => {
        rdd.foreach{
              imageArray => {
                val imageBytes = ImageBinaryTransform.decoder.decodeBuffer(imageArray._2)
               // val bais = new ByteArrayInputStream(imageBytes)
                //val bi:BufferedImage = ImageIO.read(bais)
                val sift = getImageSift(imageBytes)
                val myKmeansModel = KMeansModel.load(new SparkContext(sparkConf),"/spark/kmeansModel")
                val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
                for (i <- 0 to sift.rows()) {
                  val data = new Array[Double](sift.cols())
                  sift.row(i).get(0,0,data)
                  val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(data))
                  histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
                }

              }
            }

      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

  //获取图像的sift特征
  def getImageSift(image: Array[Byte]):Mat = {
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
    desc
  }

}
