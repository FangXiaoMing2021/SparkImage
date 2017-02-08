package com.fang.spark.demo
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fang on 17-1-19.
  */
object HiveTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HiveTest")
      .setMaster("spark://fang-ubuntu:7077")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    //    sqlContext.sql("CREATE TABLE IF NOT EXISTS kmeans (id INT, feature STRING)")
    //    sqlContext.sql("insert into table kmeans values ( 1, '0.0 0.0 0.0' )")
    //    sqlContext.sql("insert into table kmeans values ( 2, '0.1 0.1 0.1' )")
    //    sqlContext.sql("insert into table kmeans values ( 3, '0.2 0.2 0.2' )")
    //    sqlContext.sql("insert into table kmeans values ( 4, '9.0 9.0 9.0' )")
    //    sqlContext.sql("insert into table kmeans values ( 5, '9.1 9.1 9.1' )")
    //    sqlContext.sql("insert into table kmeans values ( 6, '9.2 9.2 9.2' )")
    //sqlContext.sql("insert into kmeans values ( 1, '0.0 0.0 0.0' )")
    //sqlContext.sql("LOAD DATA LOCAL INPATH 'hdfs://fang-ubuntu:9000/spark/kmeans_data.txt' INTO TABLE kmeans")
    val kmeansData = sqlContext.sql("FROM kmeans SELECT id, feature")
    kmeansData.cache()
    val parsedData = kmeansData.map { row => {
      val feature = row(1).toString
      val featureArr = feature.split(" ")
      Vectors.dense(featureArr.map(_.toDouble))
    }
    }
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    //    val WSSSE = clusters.computeCost(parsedData)
    //    println("Within Set Sum of Squared Errors = " + WSSSE)
    // Save and load model
   // clusters.save(sc, "myModelPath")
    //val sameModel = KMeansModel.load(sc, "/spark/myModelPath")
    val predictData = kmeansData.map { row => {
      val feature = row(1).toString
      val featureArr = feature.split(" ")
      val dense = Vectors.dense(featureArr.map(_.toDouble))
      val prediction = clusters.predict(dense)
      feature+" "+prediction
    }
    }.saveAsTextFile("/user/hadoop/hiveTest")

  }
}
