package ee.ut.cs

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DBSCAN {

  def showInfoMessage() {
    println("Implementation of DBSCAN clustering algorithm.")
  }

  def squaredDistance(p: (Long, (Double, Double)), q: (Long, (Double, Double))): Double = {
    Math.floor(Math.sqrt(Math.pow(q._2._1 - p._2._1, 2) + Math.pow(q._2._2 - p._2._2, 2)) * 100) / 100
  }

  def time(f: => Unit) = {
    val s = System.currentTimeMillis
    f
    System.currentTimeMillis - s
  }

  def main(args: Array[String]) {
    showInfoMessage()

    if (args.length != 3) {
      System.err.println("Usage: DBSCAN <input_file> <min_points_in_cluster> <epsilon>")
      System.exit(1)
    }
    val inputFile: String = args(0)
    val minPointsInCluster: Int = args(1).toInt
    val epsilon: Double = args(2).toDouble

    val conf = new SparkConf().setAppName("DBSCAN")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputFile)
    val points = lines
      .map(_.split(" "))
      .map(p => (p(0).trim.toDouble, p(1).trim.toDouble))
      .zipWithUniqueId().map(x => (x._2, x._1))
      .cache()

    // create all vertices
    val vertices: RDD[(VertexId, (Double, Double))] = points

    // finds distances between all points, returns ((firstPoint, secondPoint), distanceBetweenThem)
    val distanceBetweenPoints = points
      .cartesian(points)
      .filter { case (x, y) => x != y } // remove the (x, x) diagonal
      .map { case (x, y) => (x, y, squaredDistance(x, y)) }

    // filters points and keeps only those whose distance is smaller than epsilon
    val pointsWithinEps = distanceBetweenPoints.filter { case (x, y, distance) => distance <= epsilon }

    // find vertex IDs between edges and remove duplicate edges
    val edgesBetweenVertexIds = pointsWithinEps
      .map { case (x, y, distance) => (x._1, y._1) }
      .map { tuple => if (tuple._1 < tuple._2) tuple else tuple.swap }
      .distinct()

    // create all edges
    val edges:RDD[Edge[Double]] = edgesBetweenVertexIds.map(vertex => Edge(vertex._1, vertex._2))

    // create graph and find number of connected components
    val graph = Graph(vertices, edges)
    val clusters = graph.connectedComponents().vertices

    // connect clusters to points
    val pointsInCluster = clusters.innerJoin(vertices)((id, cluster, point) => (cluster, point))

    // count and leave only clusters which contain more than "minPointsInCluster"
    val numberOfClusters = pointsInCluster
      .map(_._2._1)
      .countByValue()
      .count { case t => t._2 > minPointsInCluster }

    println("Total number of clusters: " + numberOfClusters)
    sc.stop()
  }

}