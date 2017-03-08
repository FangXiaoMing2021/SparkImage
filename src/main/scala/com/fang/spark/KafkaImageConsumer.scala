package com.fang.spark

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.opencv.core.Core

import scala.util.control.Breaks._

/**
  * Created by fang on 16-12-21.
  * 从Kafka获取图像数据,计算该图像的sift直方图,和HBase中的图像直方图对比,输出最相近的图像名
  * 1488789541281
  * 1488789554300
  */
object KafkaImageConsumer {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaImageProcess")
      //.setMaster("local[4]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(sparkConf, Milliseconds(500))
    ssc.checkpoint("checkpoint")
    ssc.sparkContext.setLogLevel("WARN")
    //连接HBase参数配置
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

    //获取HBase中的图像直方图
    //TODO 是否需要过滤特征值比较少的图像?
    val hBaseRDD = ssc.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val histogramFromHBaseRDD = hBaseRDD.map {
      case (_, result) => {
        val key = Bytes.toString(result.getRow)
        val histogram = SparkUtils.deserializeArray(result.getValue("image".getBytes, "histogram".getBytes))
        (key, histogram)
      }
    }
    histogramFromHBaseRDD.cache()
    //加载kmeans模型
    val myKmeansModel = KMeansModel.load(ssc.sparkContext, SparkUtils.kmeansModelPath)

    //从Kafka接收图像数据
    val topics = Set("image_topic")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
      //"serializer.class" -> "kafka.serializer.DefaultDecoder",
      //"key.serializer.class" -> "kafka.serializer.StringEncoder"
    )

    val imageFromKafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)

    //计算kafkaStream中每个图像的特征直方图,返回图像名称和相应的特征直方图RDD
    val imageTupleDStream = imageFromKafkaDStream.map {
      imageTuple => {
        //加载Opencv库,在每个分区都需加载
        System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
        val imageBytes = imageTuple._2
        val sift = SparkUtils.getImageSift(imageBytes)
        val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
        if (!sift.isEmpty) {
          val siftByteArray = sift.get
          val siftFloatArray = SparkUtils.byteArrToFloatArr(siftByteArray)
          val size = siftFloatArray.length / 128
          for (i <- 0 to size - 1) {
            val xs: Array[Float] = new Array[Float](128)
            for (j <- 0 to 127) {
              xs(j) = siftFloatArray(i * 128 + j)
            }
            val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(xs.map(i => i.toDouble)))
            histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
          }
        }
        (imageTuple._1, imageBytes, sift, histogramArray)
      }
    }

    //imageTupleDStream.cache()
    /**
      * 保存从kafka接受的图像数据
      * object not serializable (class: org.apache.hadoop.hbase.io.ImmutableBytesWritable
      * 调换foreachRDD 和map
      */
//    imageTupleDStream.foreachRDD {
//      rdd => {
//        if (!rdd.isEmpty()) {
//          val hConfig = HBaseConfiguration.create()
//          val tableName = "imagesTest"
//          hConfig.set("hbase.zookeeper.property.clientPort", "2181")
//          hConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
//          val jobConf = new JobConf(hConfig)
//          jobConf.setOutputFormat(classOf[TableOutputFormat])
//          jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//          //保存从kafka接受的图片数据
//          rdd.map {
//            tuple => {
//              val put: Put = new Put(Bytes.toBytes(tuple._1))
//              put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"), tuple._2)
//              put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("histogram"), Utils.serializeObject(tuple._4))
//              val sift = tuple._3
//              if (!sift.isEmpty) {
//                put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"), sift.get)
//              }
//              (new ImmutableBytesWritable, put)
//            }
//          }.saveAsHadoopDataset(jobConf)
//          println("================保存============")
//        }
//      }
//    }

    val histogramFromKafkaDStream = imageTupleDStream.map(tuple => (tuple._1, tuple._4))

    val topNSimilarImageDStream = histogramFromKafkaDStream.transform {
      imageHistogramRDD => {
        //将从kafka接收的图像RDD与数据库中的图像RDD求笛卡尔积
        //返回kafka中的图像名称,和HBase数据库中图像的直方图距离sum及数据中的图像名称
        val cartesianRDD = imageHistogramRDD.cartesian(histogramFromHBaseRDD)
        val computeHistogramSum = cartesianRDD.map {
          tuple => {
            val imageHistogram = tuple._1
            val histogramHBase = tuple._2
            var sum = 0
            for (i <- 0 to imageHistogram._2.length - 1) {
              val sub = imageHistogram._2(i) - histogramHBase._2(i)
              sum = sum + sub * sub
            }
            (imageHistogram._1, (sum, histogramHBase._1))
          }
        }
        //根据接收的图像名称分组
        val groupRDD = computeHistogramSum.groupByKey()
        //对每个分组中求最相近的10张图片
        val matchImageRDD = groupRDD.map {
          tuple => {
            val top10 = Array[(Int, String)](
              (Int.MaxValue, "0"), (Int.MaxValue, "0"),
              (Int.MaxValue, "0"), (Int.MaxValue, "0"),
              (Int.MaxValue, "0"), (Int.MaxValue, "0"),
              (Int.MaxValue, "0"), (Int.MaxValue, "0"),
              (Int.MaxValue, "0"), (Int.MaxValue, "0"))
            val imageName = tuple._1
            val similarImageIter = tuple._2
            for (similarImage <- similarImageIter) {
              breakable {
                for (i <- 0 until 10) {
                  if (top10(i)._1 == Int.MaxValue) {
                    top10(i) = similarImage
                    break
                  } else if (similarImage._1 < top10(i)._1) {
                    var j = 2
                    while (j > i) {
                      top10(j) = top10(j - 1)
                      j = j - 1
                    }
                    top10(i) = similarImage
                    break
                  }
                }
              }
            }
            (imageName, top10)
          }
        }
        matchImageRDD
      }
    }
    //触发action,
//    imageTupleDStream.foreachRDD {
//      rdd => {
//        rdd.foreach {
//          imageTuple => {
//            println("============================")
//            println(imageTuple._1 )
//            println("============================")
//          }
//        }
//      }
//    }



    /**
     * 保存查询到的相似图像名称
     */
    topNSimilarImageDStream.foreachRDD {
      rdd => {
        if (!rdd.isEmpty()) {
          val similarImageTable = "similarImageTable"
          val hConfig = HBaseConfiguration.create()
          hConfig.set("hbase.zookeeper.property.clientPort", "2181")
          hConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
          val jobConf = new JobConf(hConfig)
          jobConf.setOutputFormat(classOf[TableOutputFormat])
          jobConf.set(TableOutputFormat.OUTPUT_TABLE, similarImageTable)
          rdd.map {
            tuple => {
              val put: Put = new Put(Bytes.toBytes(tuple._1))
              val tupleArray = tuple._2
              var i = 1
              for (tup <- tupleArray) {
                put.addColumn(Bytes.toBytes("similarImage"), Bytes.toBytes("image" + "_" + i), Bytes.toBytes(tup._2 + "#" + tup._1))
                i = i + 1
              }
              (new ImmutableBytesWritable, put)
            }
            //.saveAsNewAPIHadoopDataset(jobConf)
            // java.lang.NullPointerException
          }.saveAsHadoopDataset(jobConf)
          println("=======================保存相似图像=======================")
          val saveSimilarTime = System.currentTimeMillis()
          println("获得相似图像的时间"+saveSimilarTime)
          println("=======================保存相似图像=======================")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  /**
   *打印相似图像的名称
   */
  def printStreamRDD(printRDD: DStream[(String, Array[(Int, String)])]): Unit = {
    printRDD.foreachRDD {
      rdd => {
        rdd.foreach {
          imageTuple => {
            println(imageTuple._1 + " similar:")
            println("============================")
            for (i <- imageTuple._2) {
              println(i)
            }
            println("============================")
          }
        }
      }
    }
  }

}
