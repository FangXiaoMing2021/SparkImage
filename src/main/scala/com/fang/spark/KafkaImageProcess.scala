package com.fang.spark

import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, File}
import javax.imageio.ImageIO

import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by fang on 16-12-21.
  */
object KafkaImageProcess {
  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("KafkaImageProcess")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topics = Map[String,Int]("image_topic"->new Integer(1))
    //val group = "fang-group"
    //配置kafka的端口,配置value的序列化类,配置key的序列化类
    val kafkaParams=Map[String,String]("metadata.broker.list"-> "192.168.193.148:9092",
      "serializer.class"->"kafka.serializer.StringEncoder",
      "key.serializer.class"->"kafka.serializer.StringEncoder"
    )
    val kafkaImageStream:ReceiverInputDStream[(String,Array[Byte])]= KafkaUtils.createStream[String,Array[Byte],StringDeserializer,ByteArrayDeserializer](ssc,kafkaParams,topics,StorageLevel.MEMORY_AND_DISK)
    kafkaImageStream.foreachRDD{
      imageRDD =>{
        imageRDD.foreach{
          imageArray =>{
            val bi:BufferedImage = ImageIO.read(new ByteArrayInputStream(imageArray._2))
            ImageIO.write(bi,"jpg",new File("/home/fang/image"+imageArray._1))
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
