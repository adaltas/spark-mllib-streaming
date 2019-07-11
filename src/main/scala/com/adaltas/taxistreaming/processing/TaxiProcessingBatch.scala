package com.adaltas.taxistreaming.processing

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr, hour, udf}
import org.apache.spark.ml.linalg.Vectors

object TaxiProcessingBatch {

  def prepareTaxiData(dfRides: DataFrame, dfFares: DataFrame): DataFrame = {
    // Manhattan bbox
    val lonEast = -73.887
    val lonWest = -74.037
    val latNorth = 40.899
    val latSouth = 40.695
    var dfRidesManhattan = dfRides.filter(
      col("startLon") >= lonWest && col("startLon") <= lonEast &&
        col("startLat") >= latSouth && col("startLat") <= latNorth &&
        col("endLon") >= lonWest && col("endLon") <= lonEast &&
        col("endLat") >= latSouth && col("endLat") <= latNorth).
      filter(col("isStart") === "END").
      join(dfFares, expr ("""
         rideId_fares = rideId AND
          endTime > startTime AND
          endTime <= startTime + interval 2 hours
          """)).
      filter(col("tip") > 0).
      withColumn("startHour", hour(col("startTime"))).
      drop(col("rideId_fares"))

    val vectCol = udf((tip: Double) => Vectors.dense(tip))
    dfRidesManhattan.withColumn("tipVect", vectCol(dfRidesManhattan("tip")))
  }

}
