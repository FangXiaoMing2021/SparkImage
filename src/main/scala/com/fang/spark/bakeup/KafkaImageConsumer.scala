package com.fang.spark.bakeup

import com.fang.spark.SparkUtils
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.opencv.core.Core

/**
  * Created by fang on 16-12-21.
  * 从Kafka获取图像数据,计算该图像的sift直方图,和HBase中的图像直方图对比,输出最相近的图像名
  */
object KafkaImageConsumer {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("KafkaImageProcess")
      .setMaster("local[4]")
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
    val histogramMapRDD = histogramRDD.map {
      case (_, result) => {
        val key = Bytes.toString(result.getRow)
        val histogram = SparkUtils.deserializeArray(result.getValue("image".getBytes, "histogram".getBytes))
        (key, histogram)
      }
    }
    histogramMapRDD.cache()

    //加载kmeans模型
    val myKmeansModel = KMeansModel.load(ssc.sparkContext, SparkUtils.kmeansModelPath)


    val topics = Set("image_topic")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "218.199.92.225:9092,218.199.92.226:9092,218.199.92.227:9092"
      //"serializer.class" -> "kafka.serializer.DefaultDecoder",
      //"key.serializer.class" -> "kafka.serializer.StringEncoder"
    )
    val kafkaStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)

    val histogramStream = kafkaStream.map {
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
            histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex)
          }
        }
        (imageTuple._1, histogramArray)
      }
    }
    val resultImageRDD = histogramStream.transform {
      imageHistogramRDD => {
        val cartesianRDD = imageHistogramRDD.cartesian(histogramMapRDD)
        val computeHistogramSum = cartesianRDD.map{
          tuple => {
            val imageHistogram = tuple._1
            val histogramHBase = tuple._2
            var sum = 0
            for (i <- 0 to imageHistogram._2.length-1) {
              val sub = imageHistogram._2(i) - histogramHBase._2(i)
              sum = sum + sub * sub
            }
            (imageHistogram._1,(sum,histogramHBase._1))
          }
        }
 //       val matchImageRDD = computeHistogramSum.sortBy(x => x._2._1)
        //取最相似的图片
//        val matchImageRDD = computeHistogramSum.reduceByKey{
//          (x,y) =>{
//            if(x._1>y._1){
//              y
//            }else{
//              x
//            }
//          }
//        }

        val groupRDD = cartesianRDD.groupByKey()
        val matchImageRDD = groupRDD.map {
          tuple => {
            val imageHistogram = tuple._1
            val histogramIter = tuple._2
            val matchResult = histogramIter.map {
              histogramTuple => {
                var sum = 0
                val histogram = histogramTuple._2
                for (i <- 0 to histogram.length) {
                  val sub = histogram(i) - imageHistogram._2(i)
                  sum = sum + sub * sub
                }
                (sum, histogramTuple._1)
              }
            }

            (imageHistogram._1, matchResult)
          }
        }
        matchImageRDD
      }
    }

//    //打印相似图片名称
//    resultImageRDD.foreachRDD{
//      rdd => {
//        rdd.take(10).foreach {
//          imageTuple => {
//            println(imageTuple._1+" similar: "+imageTuple._2._2+" sum = "+imageTuple._2._1)
//          }
//        }
//      }
//    }
    resultImageRDD.saveAsTextFiles("./result")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


}
