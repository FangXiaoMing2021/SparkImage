package com.fang.spark.bakeup

import java.io.{ByteArrayInputStream, File, IOException}
import javax.imageio.ImageIO

import com.fang.spark.SparkUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
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
    //获取HBase中image的特征直方图RDD
    val hbaseConf = HBaseConfiguration.create()
    val tableName = "imagesTest"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
    val scan = new Scan()
    scan.addColumn(Bytes.toBytes("image"), Bytes.toBytes("histogram"))
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)
    val histogramRDD = ssc.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    histogramRDD.cache()


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
        rdd.foreach{
          imageTuple => {
            val imageBytes = imageTuple._2
            val sift = SparkUtils.getImageSiftOfMat(imageBytes)
            //加载kmeans模型
            val myKmeansModel = KMeansModel.load(new SparkContext(sparkConf), SparkUtils.kmeansModelPath)
            val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
            //计算获取的图像的sift直方图
            for (i <- 0 to sift.rows()) {
              val data = new Array[Double](sift.cols())
              sift.row(i).get(0, 0, data)
              val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(data))
              histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
            }
//            //计算图像直方图距离,并排序
//            val matchImages = histogramRDD.map {
//              case (_, result) =>
//                val key = Bytes.toString(result.getRow)
//                val histogram = SparkUtils.deserializeArray(result.getValue("image".getBytes, "histogram".getBytes))
//                var sum = 0;
//                for (i <- 0 to histogram.length) {
//                  val sub = histogram(i) - histogramArray(i)
//                  sum = sum + sub * sub
//                }
//                (sum, key)
//            }.sortByKey()
            //matchImages.take(10).foreach { tuple => println(tuple._2) }
          }
        }
      }

//      rdd => {
//        rdd.foreach {
//          imageTuple => {
//            //SparkUtils.base64StringToImage(imageArray._2,imageArray._1)
//            val imageArray = imageTuple._2
//            // println(byte.length)
//            try {
//              val bi = ImageIO.read(new ByteArrayInputStream(imageArray))
//              ImageIO.write(bi, "jpg", new File("/home/fang/imageCopy/" + imageTuple._1))
//            }
//            catch {
//              case e: IOException => {
//                e.printStackTrace()
//              }
//            }
//            //
//          }
//        }
//
//      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
