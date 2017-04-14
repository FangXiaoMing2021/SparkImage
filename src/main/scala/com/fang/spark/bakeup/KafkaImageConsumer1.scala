package com.fang.spark.bakeup

import com.fang.spark.ImagesUtil
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
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.opencv.core.Core

/**
  * Created by fang on 16-12-21.
  * 从Kafka获取图像数据,计算该图像的sift直方图,和HBase中的图像直方图对比,输出最相近的图像名
  */
object KafkaImageConsumer1 {


  def main(args: Array[String]): Unit = {
    val sparkConf = ImagesUtil.loadSparkConf("KafkaImageProcess")

    //批次间隔800ms
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

    println("============" + histogramFromHBaseRDD.count() + "===========")
    //缓存图像特征直方图库
    //TODO 解决内存不足的情况
    histogramFromHBaseRDD.cache()
    //从Kafka接收图像数据
    val topics = Set("image_topic")
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "fang-ubuntu:9092,fei-ubuntu:9092,kun-ubuntu:9092"
      //"serializer.class" -> "kafka.serializer.DefaultDecoder",
      //"key.serializer.class" -> "kafka.serializer.StringEncoder"
    )


    val imageFromKafkaDStream = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics)

    //计算kafkaStream中每个图像的特征直方图,返回图像名称和相应的特征直方图RDD
    //    val imageTupleDStream = imageFromKafkaDStream.map {
    //      imageTuple => {
    //
    //        getReceiveImageHistogram(imageTuple,myKmeansModel)
    //
    //      }
    //    }

    //println("该批次接受数据为:"+imageFromKafkaDStream.count())
    //    val imageTupleDStream = imageFromKafkaDStream.mapPartitions {
    //      iter => {
    //        val computeReceiveImageHistogramTime = System.currentTimeMillis()
    //        System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    //        val result = iter.map {
    //          imageTuple => {
    //            getReceiveImageHistogram(imageTuple, myKmeansModel)
    //          }
    //        }
    //        ImagesUtil.printComputeTime(computeReceiveImageHistogramTime, "数据量为:"+iter.size+"第一步,计算图像特征值:计算接收的每个分区的图像特征直方图时间")
    //        result
    //      }
    //    }
    val imageTupleDStream = imageFromKafkaDStream.map {
      // val computeReceiveImageHistogramTime = System.currentTimeMillis()
      System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
      imageTuple => {
        getReceiveImageHistogram(imageTuple, myKmeansModel)
      }

    }
    imageTupleDStream.cache()
    //获取接受图像的名称和特征直方图
    val histogramFromKafkaDStream = imageTupleDStream.map(tuple => (tuple._1, tuple._4))

    //    val topNSimilarImageDStream=histogramFromKafkaDStream.mapPartitions{
    //      iter=>{
    //        histogramFromHBaseRDD.map{
    //                    tuple=>{
    //                     // val size = imageHistogramArray.length
    //                      (tuple,iter)
    //                    }
    //                  }
    //      }
    //    }

    //    //获取最相似的n张图片
    val topNSimilarImageDStream = histogramFromKafkaDStream.transform {
      imageHistogramRDD => {
        println("imageHistogramRDD中的元素个数为:" + imageHistogramRDD.count())
        val imageHistogramArray = imageHistogramRDD.toLocalIterator.toArray
        println("数组长度:" + imageHistogramArray.size)
        val hbaseAndKafkaHistogram = histogramFromHBaseRDD.map {
          tuple => {
            // val size = imageHistogramArray.length
            (tuple, imageHistogramArray)
          }
        }
        getNTopSimilarImageOfAll(hbaseAndKafkaHistogram).take(10).foreach {
          tuple => {
            //println(println("tuple:"+tuple._2.length))
            for (i <- 0 to tuple._2.length - 1) {
              println("获取图像:" + tuple._2(i)._1 + " 时间为:" + System.currentTimeMillis())
            }
          }
        }
        hbaseAndKafkaHistogram
        //        val computeSimilarImageTime = System.currentTimeMillis()
        //        val result = getNTopSimilarImage(imageHistogramRDD, histogramFromHBaseRDD)
        //        ImagesUtil.printComputeTime(computeSimilarImageTime, "第二步,计算相似图像,计算每个RDD中图像的相似图像时间")
        //        result
      }
    }
    topNSimilarImageDStream.foreachRDD{
      rdd=>println(rdd.count())
    }
//    topNSimilarImageDStream.foreachRDD {
//      rdd =>
//        //println(rdd.count())
//        //val saveSimilarImageTime = System.currentTimeMillis()
//        getNTopSimilarImageOfAll(rdd).foreach {
//          tuple => {
//            for (i <- 0 to tuple._2.length - 1) {
//              println("获取图像:" + tuple._2(i)._1 + " 时间为:" + System.currentTimeMillis())
//            }
//          }
//          //ImagesUtil.printComputeTime(saveSimilarImageTime, "第三步,保存结果,保存每个RDD中相似图像的时间")
//          //        rdd.foreach{
//          //          tuple=>{
//          //            for(i<-0 to tuple._2.length-1){
//          //              println("获取图像:"+tuple._2(i)._1+" 时间为:"+System.currentTimeMillis())
//          //            }
//          //          }
//          //        }
//        }
//    }
    //
    //
    //    //保存查询到的相似图像名称
    //    topNSimilarImageDStream.foreachRDD {
    //      rdd => {
    //        if (!rdd.isEmpty()) {
    //          val saveSimilarImageTime = System.currentTimeMillis()
    //          saveSimilarImageName(rdd)
    //          ImagesUtil.printComputeTime(saveSimilarImageTime, "第三步,保存结果,保存每个RDD中相似图像的时间")
    //        }
    //      }
    //    }
    //    val saveReceiveImageTime = System.currentTimeMillis()
    //    //保存从kafka接受的图像到HBase中
    //    imageTupleDStream.foreachRDD {
    //      rdd => {
    //        saveImagesFromKafka(rdd)
    //      }
    //    }
    //    ImagesUtil.printComputeTime(saveReceiveImageTime,"保存接收相似图像时间")
    //启动程序
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  /** 保存从kafka接受的图像数据
    * object not serializable (class: org.apache.hadoop.hbase.io.ImmutableBytesWritable
    * 调换foreachRDD 和map
    *
    * @param rdd
    */
  def saveImagesFromKafka
  (rdd: RDD[(String, Array[Byte], Option[Array[Byte]], Array[Int])]) {
    if (!rdd.isEmpty()) {
      val hConfig = HBaseConfiguration.create()
      val tableName = "imagesTest"
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
          //          val sift = tuple._3
          //          if (!sift.isEmpty) {
          //            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("sift"), sift.get)
          //          }
          val harris = tuple._3
          if (!harris.isEmpty) {
            put.addColumn(Bytes.toBytes("image"), Bytes.toBytes("harris"), harris.get)
          }
          (new ImmutableBytesWritable, put)
        }
      }.saveAsHadoopDataset(jobConf)
      //println("================保存接受的图像完成==================")
    }
  }

  /**
    *
    * @param imageTuple
    * @param myKmeansModel
    * @return
    */
  def getReceiveImageHistogram(imageTuple: (String, Array[Byte]), myKmeansModel: KMeansModel)
  : (String, Array[Byte], Option[Array[Byte]], Array[Int]) = {


    //加载Opencv库,在每个分区都需加载
    //System.loadLibrary(Core.NATIVE_LIBRARY_NAME)
    val imageBytes = imageTuple._2
    //val sift = ImagesUtil.getImageSift(imageBytes)
    val sift = ImagesUtil.getImageHARRIS(imageBytes)
    val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
    if (!sift.isEmpty) {
      val siftByteArray = sift.get
      val siftFloatArray = ImagesUtil.byteArrToFloatArr(siftByteArray)
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
    //计算harris
    //   val harris = ImagesUtil.getImageHARRIS(imageBytes)
    //    if (!harris.isEmpty) {
    //      val harrisByteArray = sift.get
    //      val harrisFloatArray = ImagesUtil.byteArrToFloatArr(harrisByteArray)
    //      val size = harrisFloatArray.length / 128
    //      for (i <- 0 to size - 1) {
    //        val xs: Array[Float] = new Array[Float](128)
    //        for (j <- 0 to 127) {
    //          xs(j) = harrisFloatArray(i * 128 + j)
    //        }
    //        val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(xs.map(i => i.toDouble)))
    //        histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
    //      }
    //    }

    (imageTuple._1, imageBytes, sift, histogramArray)
  }

  /**
    *
    * @param imageHistogramRDD
    * @param histogramFromHBaseRDD
    * @return
    */
  def getNTopSimilarImage(imageHistogramRDD: RDD[(String, Array[Int])], histogramFromHBaseRDD: RDD[(String, Array[Int])]): RDD[(String, Seq[(Double, String)])] = {
    //将从kafka接收的图像RDD与数据库中的图像RDD求笛卡尔积
    //返回kafka中的图像名称,和HBase数据库中图像的直方图距离sum及数据中的图像名称
    //val getSimilarImageTime = System.currentTimeMillis()

    val cartesianRDD = imageHistogramRDD.cartesian(histogramFromHBaseRDD)
    val computeHistogramSum = cartesianRDD.map {
      tuple => {
        val imageHistogram = tuple._1
        val histogramHBase = tuple._2
        //        var sum :Double = 0.0
        //        for (i <- 0 to imageHistogram._2.length - 1) {
        //          val sub = imageHistogram._2(i) - histogramHBase._2(i)
        //          sum = sum + sub * sub
        //        }
        //获取两个向量的余弦相似度
        val sum = getArrayCosineSimilarity(imageHistogram._2, histogramHBase._2)
        (imageHistogram._1, (sum, histogramHBase._1))
      }
    }
    //根据接收的图像名称分组
    val groupRDD = computeHistogramSum.groupByKey()
    //对每个分组中求最相近的10张图片
    val matchImageRDD = groupRDD.map {
      tuple => {
        //        val top10 = Array[(Int, String)](
        //          //出现数组没有满的情况,就是相似图像出现了,名称为0的图像,程序抛出异常
        //          (Int.MaxValue, "0"), (Int.MaxValue, "0"),
        //          (Int.MaxValue, "0"), (Int.MaxValue, "0"),
        //          (Int.MaxValue, "0"), (Int.MaxValue, "0"),
        //          (Int.MaxValue, "0"), (Int.MaxValue, "0"),
        //          (Int.MaxValue, "0"), (Int.MaxValue, "0"))
        //        val imageName = tuple._1
        //        val similarImageIter = tuple._2
        //        for (similarImage <- similarImageIter) {
        //          breakable {
        //            for (i <- 0 until 10) {
        //              if (top10(i)._1 == Int.MaxValue) {
        //                top10(i) = similarImage
        //                break
        //              } else if (similarImage._1 < top10(i)._1) {
        //                var j = 2
        //                while (j > i) {
        //                  top10(j) = top10(j - 1)
        //                  j = j - 1
        //                }
        //                top10(i) = similarImage
        //                break
        //              }
        //            }
        //          }
        //        }
        val imageName = tuple._1
        val similarImageIter = tuple._2
        //        var filterSimilarImage:Iterable[(Double,String)] = null
        //        if(similarImageIter.size>500){
        //          filterSimilarImage = similarImageIter.filter(elements=>elements._1>0.5)
        //          if(filterSimilarImage.size>500){
        //            filterSimilarImage = filterSimilarImage.filter(elements=>elements._1>0.6)
        //            if(filterSimilarImage.size>500){
        //              filterSimilarImage = filterSimilarImage.filter(elements=>elements._1>0.7)
        //              if(filterSimilarImage.size>500){
        //                filterSimilarImage = filterSimilarImage.filter(elements=>elements._1>0.8)
        //                if(filterSimilarImage.size>500){
        //                  filterSimilarImage= filterSimilarImage.filter(elements=>elements._1>0.9)
        //                }
        //              }
        //            }
        //          }
        //        }else{
        //          filterSimilarImage = similarImageIter
        //        }
        //        val topN = filterSimilarImage.toSeq
        //          .sortBy(elements=>elements._1)
        //          .take(10)
        val topN = similarImageIter.toSeq.take(10)
        (imageName, topN)
      }
    }
    //ImagesUtil.printComputeTime(getSimilarImageTime,"获取相似图像的时间")
    matchImageRDD
  }


  /**
    * 保存查询到的相似图像结果
    *
    * @param rdd
    */
  def saveSimilarImageName(rdd: RDD[(String, Seq[(Double, String)])]): Unit = {
    if (!rdd.isEmpty()) {
      //      val similarImageTable = "similarImageTable"
      //      val hConfig = HBaseConfiguration.create()
      //      hConfig.set("hbase.zookeeper.property.clientPort", "2181")
      //      hConfig.set("hbase.zookeeper.quorum", "fang-ubuntu,fei-ubuntu,kun-ubuntu")
      //      val jobConf = new JobConf(hConfig)
      //      jobConf.setOutputFormat(classOf[TableOutputFormat])
      //      jobConf.set(TableOutputFormat.OUTPUT_TABLE, similarImageTable)
      //      rdd.map {
      //        tuple => {
      //          val put: Put = new Put(Bytes.toBytes(tuple._1))
      //          val tupleArray = tuple._2
      //          var i = 1
      //          for (tup <- tupleArray) {
      //            put.addColumn(Bytes.toBytes("similarImage"), Bytes.toBytes("image" + "_" + i), Bytes.toBytes(tup._2 + "#" + tup._1))
      //            i = i + 1
      //          }
      //          (new ImmutableBytesWritable, put)
      //        }
      //        //.saveAsNewAPIHadoopDataset(jobConf)
      //        // java.lang.NullPointerException
      //      }.saveAsHadoopDataset(jobConf)
      // println("处理的图片数量"+rdd.count())
      //       println("=======================保存相似图像=======================")
      //          val saveSimilarTime = System.currentTimeMillis()
      //          println("获得相似图像的时间"+saveSimilarTime)
      //TODO 把计算时间保存成文件
      //rdd.map(tuple=>tuple._1+" 获得相似图像的时间 "+System.currentTimeMillis()).saveAsTextFile("/spark/saveSimilarImageTime")
      rdd.foreach(tuple => println(tuple._1 + " 获得相似图像的时间 " + System.currentTimeMillis()))
      println("=======================保存相似图像完成=======================")
    }
  }

  /**
    * 打印相似图像的名称
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
      cosineSimilarity = arrayMultiply / (Math.sqrt(firstEuclidean) + Math.sqrt(secondEuclidean))
    }
    cosineSimilarity
  }

  def getNTopSimilarImageOfAll(hbaseAndKafkaHistogram: RDD[((String, Array[Int]), Array[(String, Array[Int])])]): RDD[((String, Array[Int]), Array[(String, Double)])] = {
    val result = hbaseAndKafkaHistogram.map {
      tuple => {
        val cosArray: Array[(String, Double)] = new Array(tuple._2.length)
        for (i <- 0 to tuple._2.length - 1) {
          cosArray(i) = (tuple._2(i) _1, getArrayCosineSimilarity(tuple._1._2, tuple._2(i)._2))
        }
        (tuple._1, cosArray)
      }
    }
    result
  }

}
