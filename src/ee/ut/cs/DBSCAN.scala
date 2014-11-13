package ee.ut.cs

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object DBSCAN {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(' ')).map(_.toDouble)
  }

  def showInfoMessage() {
    println("Implementation of DBSCAN Clustering method.")
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

    val conf = new SparkConf().setAppName("DBSCAN clustering")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(inputFile)
    val points = lines.map(parseVector)

    // map with points to trace what elements are visited
    val pointsMap = points.map(p => (p, 1)).collectAsMap()

    // finds distances between all points, returns ((firstPoint, secondPoint), distanceBetweenThem)
    val distanceBetweenPoints = points
      .cartesian(points)
      .filter{ case (x, y) => x != y } // remove the (x, x) diagonal
      .map{ case (x,y) => ((x, y), squaredDistance(x, y))
    }

    // filters points and keeps only those whose distance is smaller than epsilon
    val pointsWithinEps = distanceBetweenPoints.filter {
      case ((x, y), distance) => distance <= epsilon
    }

    // puts x coordinate so that later it can be grouped by x coordinate
    val pointsWithinEpsByX = pointsWithinEps.map{
      case ((x,y),distance) => (x,(y,distance))
    }.cache()

    // groups by x coordinate
    val xCoordinatesWithDistance = pointsWithinEpsByX.groupByKey().collect()

    pointsMap.foreach(println)

    xCoordinatesWithDistance.foreach({ p =>
      println("Point:" + p)
      println("Map:" + pointsMap(p._1))
    })

    sc.stop()
  }

}