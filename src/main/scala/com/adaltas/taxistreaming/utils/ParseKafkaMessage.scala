package com.adaltas.taxistreaming.utils

import org.apache.spark.sql.functions.split
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

object ParseKafkaMessage {
  def parseDataFromKafkaMessage(sdf: DataFrame, schema: StructType): DataFrame = {
    assert(sdf.isStreaming == true) //DataFrame doesn't receive treaming data"
    var res = sdf
    val col = split(sdf("value"), ",") // split attributes to nested array in one Column
    //now expand col to multiple top-level columns
    for ((field, idx) <- schema.zipWithIndex) {
      res = res.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    }
  res.drop("value")
  }

}