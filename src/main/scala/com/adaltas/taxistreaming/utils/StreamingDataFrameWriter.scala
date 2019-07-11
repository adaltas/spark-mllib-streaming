package com.adaltas.taxistreaming.utils

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{year, month, dayofmonth}
import org.apache.spark.sql.streaming.StreamingQuery

object StreamingDataFrameWriter {

  def StreamingDataFrameConsoleWriter(sdf: DataFrame, queryName: String): StreamingQuery = {
    //for the TipsInConsole query
    sdf.writeStream.
      queryName(queryName).
      outputMode("append").
      format("console").
      option("truncate", false).
      start()
  }

  def StreamingDataFrameMemoryWriter(sdf: DataFrame, queryName: String): StreamingQuery = {
    //for the TipsInMemory query
    sdf.writeStream.
      queryName(queryName).
      outputMode("append").
      format("memory").
      option("truncate", false).
      start()
  }

  def StreamingDataFrameHdfsWriter(sdf: DataFrame, queryName: String): StreamingQuery = {
    //for the PersistRawTaxiRides and PersistRawTaxiFares queries
    sdf.withColumn("year", year(col("startTime")))
    .withColumn("month", month(col("startTime")))
    .withColumn("day", dayofmonth(col("startTime")))
    .writeStream
    .queryName(queryName)
    .outputMode("append")
    .format("parquet")
    .option("path", "datalake/RidesRaw")
    .option("checkpointLocation", "checkpoints/RidesRaw")
    .partitionBy("startTime")
    .option("truncate", false)
    .start()
  }

}
