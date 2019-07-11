package com.adaltas.taxistreaming.clustering

import com.adaltas.taxistreaming.utils.ClustersGeoJSON
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, expr, hour, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.adaltas.taxistreaming.processing.TaxiProcessingBatch

object MainKmeans {

  def prepareKmeansPipeline(k: Int): Pipeline = {

    val tipScaler = new MinMaxScaler()
      .setInputCol("tipVect")
      .setOutputCol("tipScaled")
      .setMin(0)
      .setMax(1)

    val featuresAssembler = new VectorAssembler()
      .setInputCols(Array("startLon", "startLat", "tipScaled"))
      .setOutputCol("features")

    val kmeansEstimator = new KMeans().setK(k).setSeed(1L)

    new Pipeline().setStages(Array(tipScaler, featuresAssembler, kmeansEstimator))
  }

  def computeVariances(predictions: DataFrame): DataFrame = {
    /* Calculate variance for each cluster */
    import org.apache.spark.ml.stat.Summarizer

    val variances = predictions.groupBy(col("prediction")).agg(Summarizer.variance(col("features")).alias("clustersVar"))
    val vecToSeq = udf((v: org.apache.spark.ml.linalg.Vector) => v.toArray)
    val assemblerVarianceVect = new VectorAssembler().setInputCols(Array("prediction", "clustersVar")).setOutputCol("vect")
    val variancesMerged = assemblerVarianceVect.transform(variances).select("vect")
    // Prepare a list of columns to create from the Vector
    val cols: Seq[String] = Seq("prediction", "startLonVar", "startLatVar", "tipScaledVar")
    val exprs = cols.zipWithIndex.map{ case (c, i) => col("_tmp").getItem(i).alias(c) }
    variancesMerged.select(vecToSeq(col("vect")).alias("_tmp")).select(exprs:_*) //variancesDF
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Part 4")
      .master("local")
      .getOrCreate()

    val dfRides: DataFrame = spark.read.parquet("datalake/RidesRaw")
      .selectExpr("rideId","endTime","startLon",
        "startLat","endLon", "endLat")

    val dfFares: DataFrame = spark.read.parquet("datalake/FaresRaw")
      .selectExpr("rideId AS rideId_fares", "startTime", "tip")

    val dfRidesManhattan = TaxiProcessingBatch.prepareTaxiData(dfRides, dfFares)

    //keep original max tip for rescaling later on
    val originalTipMax = dfRidesManhattan
      .agg(org.apache.spark.sql.functions.max("tip")).collect()(0).getFloat(0)

    val evaluatorSilhouette = new ClusteringEvaluator()

    val k = 12
    val startingHour = 0
    val endingHour = 24
    for (h <- startingHour until endingHour) {
      val pipeline = prepareKmeansPipeline(k)
      val datasetHour = dfRidesManhattan.filter(col("startHour") === h)
      val kmeansPipe = pipeline.fit(datasetHour) // obtain a PipelineModel
      //save with MLWriter
      kmeansPipe.write.overwrite.save(s"kmeans-models/clusters-at-$h")
    }

     /*Example below was used to generate GeoJSON at 10h
      val pipeline = prepareKmeansPipeline(k)
      val datasetHour = dfRidesManhattan.filter(col("startHour") === 10)
      val kmeansPipe = pipeline.fit(datasetHour) // obtain a PipelineModel
      val centers = kmeansPipe.stages(2).asInstanceOf[KMeansModel].clusterCenters
      //centers.foreach(println)

      val predictions = kmeansPipe.transform(datasetHour) //same data
      val silhouette = evaluatorSilhouette.evaluate(predictions)
      //println(s"Silhouette with squared euclidean distance = $silhouette at k=$k")

      //compute variances, prepare geojson file and write it to a file

      val variancesDF = computeVariances(predictions)
      import java.io.{File, PrintWriter}
      val writer = new PrintWriter(new File("clusters-mean.geojson"))
      writer.write(ClustersGeoJSON.generateGeoJSON(centers, variancesDF, originalTipMax, k, silhouette).toString())
      writer.close()
      */


    spark.stop()
  }

}
