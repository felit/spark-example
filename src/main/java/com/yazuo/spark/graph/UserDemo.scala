package com.yazuo.spark.graph

import org.apache.spark.SparkContext
import  org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 */
object UserDemo extends App{
  val sc: SparkContext = new SparkContext("local", "rdd-create")
  val users: RDD[(VertexId, (String, String))] =
    sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  // Create an RDD for edges
  val relationships: RDD[Edge[String]] =
    sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
  // Define a default user in case there are relationship with missing user
  val defaultUser = ("John Doe", "Missing")
  // Build the initial Graph
  val graph = Graph(users, relationships, defaultUser)
  graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count
  graph.edges.filter(e => e.srcId > e.dstId).count
  println(s"count:${graph.edges.filter(e => e.srcId < e.dstId).count}")
}
