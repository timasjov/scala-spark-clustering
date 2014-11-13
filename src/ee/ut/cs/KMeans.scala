import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object KMeans {

  def main(args: Array[String]) {
    showInfoMessage()

    if (args.length < 3) {
      System.err.println("Usage: KMeans <file> <number_of_clusters> <converge_distance>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(sparkConf)

    val points = sc.textFile(args(0)).map(parseVector).cache()
    val numberOfClusters = args(1).toInt
    val convergeDistance = args(2).toDouble

    // take randomly k points
    val centers = points.takeSample(withReplacement = false, numberOfClusters, System.nanoTime.toInt).toArray
    var distanceBetweenOldAndNewCentres = 1.0
    var iterations = 0

    while (distanceBetweenOldAndNewCentres > convergeDistance) {

      // for each point find nearest centre index
      // return index, point itself and number of reps
      val closestCenter = points.map(p =>
        (closestPoint(p, centers), (p, 1))
      )

      // reduce by closest centre
      // sum all reduces points x, y coordinates and their quantity
      val aggregatedPoints = closestCenter.reduceByKey {
        case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2)
      }

      // calculate positions of new centres
      val newCentres = aggregatedPoints.map { aggregatedPoint =>
        (aggregatedPoint._1, aggregatedPoint._2._1 * (1.0 / aggregatedPoint._2._2))
      }.collectAsMap()

      // calculate converge distance between old and new clusters
      // and assign new centre values to old centres
      distanceBetweenOldAndNewCentres = 0.0
      for (i <- 0 until numberOfClusters) {
        distanceBetweenOldAndNewCentres += squaredDistance(centers(i), newCentres(i))
        centers(i) = newCentres(i)
      }
      iterations += 1
    }

    println("Number of iterations: " + iterations)
    sc.stop()
  }

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(" ").map(_.toDouble))
  }

  def closestPoint(point: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val maxDistance = squaredDistance(point, centers(i))
      if (maxDistance < closest) {
        closest = maxDistance
        bestIndex = i
      }
    }
    bestIndex
  }

  def time(f: => Unit) = {
    val s = System.currentTimeMillis
    f
    System.currentTimeMillis - s
  }

  def showInfoMessage() {
    println("Implementation of KMeans clustering algorithm.")
  }

}