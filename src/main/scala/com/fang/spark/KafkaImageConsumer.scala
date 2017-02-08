package com.fang.spark

import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

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

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    kafkaStream.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          partition => {
            //            val conf = HBaseConfiguration.create()
            //            val conn = ConnectionFactory.createConnection(conf)
            //            val userTable = TableName.valueOf("tablename")
            //            val table = conn.getTable(userTable)

            partition.foreach {
              imageArray => {
                val imageBytes = ImageBinaryTransform.decoder.decodeBuffer(imageArray._2)
                val sift = SparkUtils.getImageSift(imageBytes)
                val myKmeansModel = KMeansModel.load(new SparkContext(sparkConf), "/spark/kmeansModel")
                val histogramArray = new Array[Int](myKmeansModel.clusterCenters.length)
                //计算获取的图像的sift直方图
                for (i <- 0 to sift.rows()) {
                  val data = new Array[Double](sift.cols())
                  sift.row(i).get(0, 0, data)
                  val predictedClusterIndex: Int = myKmeansModel.predict(Vectors.dense(data))
                  histogramArray(predictedClusterIndex) = histogramArray(predictedClusterIndex) + 1
                }

                //计算图像直方图距离,并排序
                val matchImages = histogramRDD.map {
                  case (_, result) =>
                    val key = Bytes.toString(result.getRow)
                    val histogram = SparkUtils.deserializeArray(result.getValue("image".getBytes, "histogram".getBytes))
                    var sum = 0;
                    for (i <- 0 to histogram.length) {
                      val sub = histogram(i) - histogramArray(i)
                      sum = sum + sub * sub
                    }
                    (sum, key)
                }.sortByKey()
                matchImages.take(10).foreach { tuple => println(tuple._2) }
              }
            }

          }
        }

      }
    }
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


}
