package com.fang.spark.bakeup

import java.io.{ByteArrayInputStream, File, IOException}
import javax.imageio.ImageIO
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by fang on 16-12-21.
  */
object KafkaImageConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaImageProcess").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")
    // val topics = Map[String, Int]("image_topic" -> new Integer(1))
    val topics = Set("image_topic")
    //val group = "fang-group"
    //配置kafka的端口,配置value的序列化类,配置key的序列化类
    //    val kafkaParams = Map[String, String](
    //      "metadata.broker.list" -> "192.168.193.148:9092",
    //      "serializer.class" -> "kafka.serializer.StringEncoder",
    //      "key.serializer.class" -> "kafka.serializer.StringEncoder"
    //    )
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
      //"serializer.class" -> "kafka.serializer.DefaultDecoder",
      //"key.serializer.class" -> "kafka.serializer.StringEncoder"
    )
    //    val kafkaImageStream: ReceiverInputDStream[(String, Array[Byte])] = KafkaUtils
    //      .createStream[String, Array[Byte], StringDeserializer, ByteArrayDeserializer](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK)
    //    kafkaImageStream.foreachRDD {
    //      imageRDD => {
    //        imageRDD.foreach {
    //          imageArray => {
    //            val bi: BufferedImage = ImageIO.read(new ByteArrayInputStream(imageArray._2))
    //            ImageIO.write(bi, "jpg", new File("/home/fang/image" + imageArray._1))
    //          }
    //        }
    //      }
    //    }
    //StorageLevel.MEMORY_AND_DISK_2
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)
    kafkaStream.foreachRDD {
      rdd => {
        rdd.foreach {
          imageTuple => {
            //SparkUtils.base64StringToImage(imageArray._2,imageArray._1)
            val imageArray = imageTuple._2
            // println(byte.length)
            try {
              val bi = ImageIO.read(new ByteArrayInputStream(imageArray))
              ImageIO.write(bi, "jpg", new File("/home/fang/imageCopy/" + imageTuple._1))
            }
            catch {
              case e: IOException => {
                e.printStackTrace()
              }
            }
            //
          }
        }

      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
