package com.adaltas.taxistreaming

import org.apache.spark.sql.functions.{col, dayofmonth, month, year}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Local fs data writer")
      .master("local")
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

    var dfRides = spark.read
      .option("header", false)
      .option("inferSchema", false)
      .schema(taxiRidesSchema)
      .csv("nycTaxiRides.gz")

    var dfFares = spark.read
      .option("header", false)
      .option("inferSchema", false)
      .schema(taxiFaresSchema)
      .csv("nycTaxiFares.gz")

    dfRides
      .withColumn("year", year(col("startTime")))
      .withColumn("month", month(col("startTime")))
      .withColumn("day", dayofmonth(col("startTime")))
      .write
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save("datalake/RidesRaw")

    dfFares
      .withColumn("year", year(col("startTime")))
      .withColumn("month", month(col("startTime")))
      .withColumn("day", dayofmonth(col("startTime")))
      .write
      .format("parquet")
      .partitionBy("year", "month", "day")
      .save("datalake/FaresRaw")

    spark.stop()

  }
}