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
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.opencv.core.Core

/**
  * Created by fang on 16-12-21.
  * 从Kafka获取图像数据,计算该图像的sift直方图,和HBase中的图像直方图对比,输出最相近的图像名
  */
object KafkaImageConsumer {


  def main(args: Array[String]): Unit = {
    val sparkConf = ImagesUtil.loadSparkConf("KafkaImageProcess")

    //批次间隔1s
    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))
    ssc.checkpoint("checkpoint")
    ssc.sparkContext.setLogLevel("WARN")
    //连接HBase参数配置
    val hbaseConf = ImagesUtil.loadHBaseConf()
    val tableName = ImagesUtil.imageTableName
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)

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

    //加载kmeans模型
    val myKmeansModel = KMeansModel.load(ssc.sparkContext, ImagesUtil.kmeansModelPath)

    //获取HBase中图像的特征直方图
    val histogramFromHBaseRDD = hBaseRDD.map {
      case (_, result) => {
        val key = Bytes.toString(result.getRow)
        val histogram = ImagesUtil.deserializeArray(result.getValue("image".getBytes, "histogram".getBytes))
        (key, histogram)
      }
    }

    //缓存图像特征直方图库
    //TODO 解决内存不足的情况
    histogramFromHBaseRDD.cache()
    println("============" + histogramFromHBaseRDD.count() + "===========")

    //从Kafka接收图像数据
    val topics = Set("image_topic")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "fang-ubuntu:9092,fei-ubuntu:9092,kun-ubuntu:9092"
    )
    //获取Kafka中图像数据
    val imageFromKafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)
    //println("该批次数据为:"+imageFromKafkaDStream.count())
    val imageTupleDStream = imageFromKafkaDStream.mapPartitions {
      System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
      iter => {
        iter.map {
          imageTuple => {
            getReceiveImageHistogram(imageTuple, myKmeansModel)
          }
        }
      }
    }
    //    val imageTupleDStream = imageFromKafkaDStream.map {
    //      //System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    //      imageTuple => {
    //        getReceiveImageHistogram(imageTuple, myKmeansModel)
    //      }
    //    }
    //imageTupleDStream.cache()
    //获取接受图像的名称和特征直方图
    val histogramFromKafkaDStream = imageTupleDStream.map(tuple => (tuple._1, tuple._4))
    //    //获取最相似的n张图片
    val topNSimilarImageDStream = histogramFromKafkaDStream.transform {
      imageHistogramRDD => {
        val imageHistogramArray = imageHistogramRDD.toLocalIterator.toVector
        val sizeOfBatchImage = imageHistogramArray.length
        println("imageHistogramRDD中的元素个数为:" + sizeOfBatchImage)
        val hbaseAndKafkaHistogram = histogramFromHBaseRDD.map {
          tuple => {
            val hbaseAndKafkaTupleArray = new Array[((String, Array[Int]), (String, Array[Int]))](sizeOfBatchImage)
            for (i <- 0 to sizeOfBatchImage - 1) {
              hbaseAndKafkaTupleArray(i) = (imageHistogramArray(i), tuple)
            }
            hbaseAndKafkaTupleArray
          }
        }.flatMap(arr => arr)
        //获得接收的每张图像和HBase中图像的相似度
        val similarImageGroup = getNTopSimilarImageOfAll(hbaseAndKafkaHistogram, sizeOfBatchImage).groupByKey().map {
          tuple => {
            (tuple._1, tuple._2.toSeq.sortBy(_._2).reverse.take(10))
          }
        }
        //打印获得相似图像的时间
        similarImageGroup.foreach {
          tuple => println("获取图像:" + tuple._1 + " 时间为:" + System.currentTimeMillis())
        }
        similarImageGroup
      }

    }

    topNSimilarImageDStream.foreachRDD{
      rdd=>saveSimilarImageName(rdd)
    }
    imageTupleDStream.foreachRDD{
      rdd=>saveImagesFromKafka(rdd)
    }
//    println("该批次数据为:"+histogramFromKafkaDStream.count())
//        topNSimilarImageDStream.foreachRDD {
//          // var i:Long = 0
//          rdd => {
//            println("完成")
//          }
//        }

    //启动程序
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  /**
    *
    * @param imageTuple
    * @param myKmeansModel
    * @return
    */
  def getReceiveImageHistogram(imageTuple: (String, Array[Byte]), myKmeansModel: KMeansModel)
  : (String, Array[Byte], Option[Array[Array[Double]]], Array[Int]) = {
    //加载Opencv库,在每个分区都需加载
    //System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    val imageBytes = imageTuple._2
    //val harris = Utils.getImageHARRISTwoArray(imageBytes)
    val harris = Utils.getImageHARRISTwoDim(imageBytes)
    val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
    if (harris.length != 0) {
      for (i <- 0 to harris.length - 1) {
        val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(harris(i)))
        histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
      }
      (imageTuple._1, imageBytes, Some(harris), histogramArray)
      //(imageTuple._1, imageBytes, None, histogramArray)
    } else {
      (imageTuple._1, imageBytes, None, histogramArray)
    }

  }

  /**
    * 计算两个向量的相似度,使用余弦
    *
    * @param arrayFirst
    * @param arraySecond
    * @return
    */
  def getArrayCosineSimilarity(arrayFirst: Array[Int], arraySecond: Array[Int]): Double = {
    //第一个向量的长度
    var firstEuclidean: Double = 0.0
    //第二个向量的长度
    var secondEuclidean: Double = 0.0
    //两个向量的乘积
    var arrayMultiply: Double = 0.0
    //两个向量的相似度
    var cosineSimilarity: Double = -1.0
    if (arrayFirst.length == arraySecond.length) {
      for (i <- 0 to arrayFirst.length - 1) {
        firstEuclidean = Math.pow(arrayFirst(i), 2) + firstEuclidean
      }
      for (j <- 0 to arraySecond.length - 1) {
        secondEuclidean = arraySecond(j) * arraySecond(j) + secondEuclidean
      }
      for (m <- 0 to arrayFirst.length - 1) {
        arrayMultiply = arrayMultiply + arrayFirst(m) * arraySecond(m)
      }
      cosineSimilarity = arrayMultiply / (Math.sqrt(firstEuclidean) * Math.sqrt(secondEuclidean))
    }
    cosineSimilarity
  }


  /**
    * 计算接收的每张图像和HBase中的图像的余弦值
    * 过滤值较小的
    *
    * @param hbaseAndKafkaHistogram
    * @param sizeOfBatchImage
    * @return
    */
  def getNTopSimilarImageOfAll(hbaseAndKafkaHistogram: RDD[((String, Array[Int]), (String, Array[Int]))], sizeOfBatchImage: Long): RDD[(String, (String, Double))] = {
    val cosineOfReceiveImage = hbaseAndKafkaHistogram.map {
      tuple => {
        val cosine = getArrayCosineSimilarity(tuple._1._2, tuple._2._2)
        (tuple._1._1, (tuple._2._1, cosine))
      }
    }.filter {
      cosineTuple => cosineTuple._2._2 > 0.95
    }
    cosineOfReceiveImage
  }

  /**
    * 保存查询到的相似图像结果
    *
    * @param rdd
    */
  def saveSimilarImageName(rdd: RDD[(String, Seq[(String, Double)])]): Unit = {
    if (!rdd.isEmpty()) {
      val similarImageTable = ImagesUtil.similarImageTableName
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
            put.addColumn(Bytes.toBytes("similarImage"), Bytes.toBytes("image" + "_" + i), Bytes.toBytes(tup._1 + "#" + tup._2))
            i = i + 1
          }
          (new ImmutableBytesWritable, put)
        }
      }.saveAsHadoopDataset(jobConf)
      //rdd.map(tuple=>tuple._1+" 获得相似图像的时间 "+System.currentTimeMillis()).saveAsTextFile("/spark/saveSimilarImageTime")
      //rdd.foreach(tuple => println(tuple._1 + " 获得相似图像的时间 " + System.currentTimeMillis()))
      println("=======================保存相似图像完成=======================")
    }
  }

  /** 保存从kafka接受的图像数据
    * object not serializable (class: org.apache.hadoop.hbase.io.ImmutableBytesWritable
    * 调换foreachRDD 和map
    *
    * @param rdd
    */
  def saveImagesFromKafka
  (rdd: RDD[(String, Array[Byte], Option[Array[Array[Double]]], Array[Int])]) {
    if (!rdd.isEmpty()) {
      val hConfig = HBaseConfiguration.create()
      val tableName = ImagesUtil.imageTableName
      hConfig.set("hbase.zookeeper.property.clientPort", "2181")
      hConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
      val jobConf = new JobConf(hConfig)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      //保存从kafka接受的图片数据
      rdd.map {
        tuple => {
          val put: Put = new Put(Bytes.toBytes(tuple._1))
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("binary"), tuple._2)
          //TODO 改了序列化
          put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("histogram"), ImagesUtil.ObjectToBytes(tuple._4))
          val harris = tuple._3
          if (!harris.isEmpty) {
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("harris"), ImagesUtil.ObjectToBytes(harris.get))
          }
          (new ImmutableBytesWritable, put)
        }
      }.saveAsHadoopDataset(jobConf)
      println("================保存接受的图像完成==================")
    }
  }

}
