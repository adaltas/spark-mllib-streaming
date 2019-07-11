package com.adaltas.taxistreaming

import com.adaltas.taxistreaming.clustering.MainKmeans.prepareKmeansPipeline
import com.adaltas.taxistreaming.processing.TaxiProcessing
import com.adaltas.taxistreaming.utils.{ParseKafkaMessage, StreamingDataFrameWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, hour, udf}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeansModel

/*
spark-submit \
  --master yarn --deploy-mode client \
  --class com.adaltas.taxistreaming.MainConsoleClustering \
  --num-executors 2 --executor-cores 1 \
  --executor-memory 5g --driver-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
  --conf spark.sql.hive.thriftServer.singleSession=true \
  /vagrant/taxi-streaming-scala_2.11-0.1.0-SNAPSHOT.jar

* The application reads data from Kafka topic, parses Kafka messages, processes it, and prints the results in console
* `TipsClustersInConsole` query writes the streaming results to stdout of the Spark Driver
*/

//--class com.adaltas.taxistreaming.MainConsoleClustering --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1
object MainConsoleClustering {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Spark Streaming part 4: clustering")
      .getOrCreate()

    val taxiRidesSchema = StructType(Array(
      StructField("rideId", LongType), StructField("isStart", StringType),
      StructField("endTime", TimestampType), StructField("startTime", TimestampType),
      StructField("startLon", FloatType), StructField("startLat", FloatType),
      StructField("endLon", FloatType), StructField("endLat", FloatType),
      StructField("passengerCnt", ShortType), StructField("taxiId", LongType),
      StructField("driverId", LongType)))

    val taxiFaresSchema = StructType(Seq(
      StructField("rideId", LongType), StructField("taxiId", LongType),
      StructField("driverId", LongType), StructField("startTime", TimestampType),
      StructField("paymentType", StringType), StructField("tip", FloatType),
      StructField("tolls", FloatType), StructField("totalFare", FloatType)))

    //"master02.cluster:6667" <-> "localhost:9997"
    var sdfRides = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "master02.cluster:6667").
      option("subscribe", "taxirides").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    var sdfFares= spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "master02.cluster:6667").
      option("subscribe", "taxifares").
      option("startingOffsets", "latest").
      load().
      selectExpr("CAST(value AS STRING)")

    sdfRides = ParseKafkaMessage.parseDataFromKafkaMessage(sdfRides, taxiRidesSchema)
    sdfFares= ParseKafkaMessage.parseDataFromKafkaMessage(sdfFares, taxiFaresSchema)
    sdfRides = TaxiProcessing.cleanRidesOutsideNYC(sdfRides)
    sdfRides = TaxiProcessing.removeUnfinishedRides(sdfRides)
    val sdf = sdfRides.withColumn("hour", hour(col("endTime")))

    //read clusters
    var hourlyClusters: Array[Array[org.apache.spark.ml.linalg.Vector]] = Array()
    val startingHour = 0
    val endingHour = 24
    for (h <- startingHour until endingHour) {
      val reloadedKmeansPipe: PipelineModel = PipelineModel
        .load(s"kmeans-models/clusters-at-$h")
      val centers: Array[org.apache.spark.ml.linalg.Vector] = reloadedKmeansPipe
        .stages(2)
        .asInstanceOf[KMeansModel]
        .clusterCenters
      hourlyClusters = hourlyClusters :+ centers
    }


    def distBetween(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
      //distance between (lon1, lat1) and (lon2, lat2) in meters
      val earthRadius = 6371000 //meters
      val dLon = Math.toRadians(lon2 - lon1)
      val dLat = Math.toRadians(lat2 - lat1)
      val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(dLon / 2) * Math.sin(dLon / 2)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
      val dist = (earthRadius * c).toFloat
      dist
    }

    val RecommendedLonLat = udf { (h: Int, lon: Double, lat: Double) => {
      val clustersArray = hourlyClusters(h)
      var closestCluster = clustersArray(0) //init to 1st cluster
      clustersArray foreach { case (vect) =>
        val clusterLon = vect(0)
        val clusterLat = vect(1)
        val clusterTip = vect(2)
        val dist = distBetween(clusterLon, clusterLat, lon, lat)
        val currentBestDist = distBetween(closestCluster(0), closestCluster(1), lon, lat)
        if ((dist < currentBestDist) && (clusterTip > closestCluster(2))) {
          closestCluster = vect
        }
      }
      Seq(closestCluster(0), closestCluster(1))
    }: Seq[Double] }
    //// otherwise udf could return just closestCluster of org.apache.spark.ml.linalg.Vector type

    val sdfRes = sdf
      .withColumn("RecommendedLonLat", RecommendedLonLat(
      col("hour"), col("endLon"), col("endLat")))
      .drop(col("passengerCnt"))

    // Write streaming results in console
    StreamingDataFrameWriter.StreamingDataFrameConsoleWriter(sdfRes, "TipsClustersInConsole").awaitTermination()

    spark.stop()
  }

}
