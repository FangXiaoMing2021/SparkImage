package com.fang.spark.exampleCode

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

/**
  * Created by fang on 17-1-15.
  */

object PropertyGraphTest {

  def main(args: Array[String]): Unit = {
    //val userGraph: Graph[(String, String), String] = null
    // Assume the SparkContext has already been constructed
    val sparkConf = new SparkConf()
      .setAppName("PropertyGraphTest")
      //.setMaster("local[*]")
      //.setMaster("spark://fang-ubuntu:7077")
    val sc: SparkContext = new SparkContext(sparkConf)

    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(
        Array(
          (3L, ("rxin", "student")),
          (7L, ("jgonzal", "postdoc")),
          (5L, ("franklin", "prof")),
          (2L, ("istoica", "prof"))
        )
      )

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(
        Array(
          Edge(3L, 7L, "collab"),
          Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"),
          Edge(5L, 7L, "pi")
        )
      )

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    //val graph: Graph[(String, String), String] // Constructed from above
    // Count all users which are postdocs
    graph.vertices.filter {
      case (id, (name, pos)) => pos == "postdoc"
    }.count
    // Count all the edges where src > dst
    graph.edges.filter(e => e.srcId > e.dstId).count

    graph.edges.filter {
      case Edge(src, dst, prop) => src > dst
    }.count
    // Use the triplets view to create an RDD of facts.
    val facts: RDD[String] =
      graph.triplets.map(triplet =>
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
      )
    facts.collect.foreach(println(_))

  }

}
