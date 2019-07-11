package com.adaltas.taxistreaming.processing

import com.adaltas.taxistreaming.utils.PointInPoly
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, udf}

object TaxiProcessing {

  def cleanRidesOutsideNYC(dfRides: DataFrame): DataFrame = {
    val lonEast = -73.7
    val lonWest = -74.05
    val latNorth = 41.0
    val latSouth = 40.5
    dfRides.filter(
      col("startLon") >= lonWest && col("startLon") <= lonEast &&
      col("startLat") >= latSouth && col("startLat") <= latNorth &&
      col("endLon") >= lonWest && col("endLon") <= lonEast &&
      col("endLat") >= latSouth && col("endLat") <= latNorth
      )
  }

  def removeUnfinishedRides(dfRides: DataFrame): DataFrame = {
    dfRides.filter(col("isStart") === "END") //Keep only finished!
  }

  def joinRidesWithFares(dfRides: DataFrame, dfFares: DataFrame): DataFrame = {
    //Apply watermarks on event -time columns
    val dfFaresWithWatermark = dfFares
    .selectExpr("rideId AS rideId_fares", "startTime", "totalFare", "tip")
    .withWatermark("startTime", "30 minutes") //maximal delay

    val dfRidesWithWatermark = dfRides
      .selectExpr("rideId", "endTime", "driverId", "taxiId",
      "startLon", "startLat", "endLon", "endLat")
      .withWatermark("endTime", "30 minutes")

    //return join with event - time constraints and aggregate
    var df = dfFaresWithWatermark.join(dfRidesWithWatermark,
      expr ("""
       rideId_fares = rideId AND
        endTime > startTime AND
        endTime <= startTime + interval 2 hours
        """))
    df.drop(col("rideId_fares"))
  }

  def appendStartEndNeighbourhoods(df: DataFrame, sparks: SparkSession): DataFrame = {
    // Feature engineering neighborhoods

    val lookupTable = sparks.read.json("file:///vagrant/NYC_neighborhoods/nbhd.jsonl").select("name", "coord")
      .collect().map(row => ( row.getAs[String](0) -> row.getSeq[Seq[Double]](1) )).toMap
    val broadcastVar = sparks.sparkContext.broadcast(lookupTable) //use broadcastVar.value from now on
    val manhattanBbox: Vector[(Double, Double)] = Vector(
      (-74.0489866963, 40.681530375),
      (-73.8265135518, 40.681530375),
      (-73.8265135518, 40.9548628598),
      (-74.0489866963, 40.9548628598),
      (-74.0489866963, 40.681530375)
    )
    val findNeighbourhoodUDF = udf { (lon: Double, lat: Double) => {
      /*takes geo point as lon, lat floats and returns name of neighborhood it belongs to
      needs broadcastVar available*/
      var nbhd: String = "Other" //default
      broadcastVar.value foreach { case (key, value) =>
        if (PointInPoly.isPointInPoly(lon, lat, value)) {
          nbhd = key
        }
      }
      nbhd
    }}

    df
      .withColumn("stopNbhd", findNeighbourhoodUDF(col("endLon"), col("endLat")))
      .withColumn("startNbhd", findNeighbourhoodUDF(col("startLon"), col("startLat")))

  }

}