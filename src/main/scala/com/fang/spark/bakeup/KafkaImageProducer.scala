package com.fang.spark.bakeup

import java.io._
import java.util.HashMap

import com.fang.spark.ImageBinaryTransform
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

/**
  * Created by fang on 16-12-21.
  */
object KafkaImageProducer {
  def main(args: Array[String]) {
    val topic = "image_topic"
    val brokers = "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
    // Zookeeper connection properties
    // 生产端配置的是kafka的ip和端口
    val props = new HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//      "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    val fileList = new File("/home/fang/imageTest").listFiles()
    for(file<-fileList){
//      val bi:BufferedImage= ImageIO.read(file)
//      val image = bi.getRaster.getDataBuffer.asInstanceOf[DataBufferByte].getData
      val image = ImageBinaryTransform.getImageBinary(file)
      val message = new ProducerRecord[String, String](topic, file.getName, image)
      println(file.getName)
      println(image.length)
      producer.send(message)
      Thread.sleep(100)
    }
    producer.close()
  }
}