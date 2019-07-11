package com.adaltas.taxistreaming.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

object ClustersGeoJSON {

  def generateGeoJSON(centers: Array[org.apache.spark.ml.linalg.Vector],
                      variancesDF: DataFrame, originalTipMax: Double,
                      k: Int, silhouette: Double): io.circe.Json = {

    //scan Cluster Centers to get max tip
    var maxTip: Double = 0
    for (vect <- centers) {
      val tip = vect(2) //Array[Double]
      if (tip > maxTip) maxTip = tip
    }

    //calc coeff to scale maxTip to max val of 255 for marker color coding
    // maxTip*alfa = 255
    val alfa = 255/maxTip

    //calc coeff to rescalled maxTip back to originalTipMax
    // maxTip*beta = originalTipMax
    val beta = originalTipMax/maxTip

    //GeoJSON structure
    case class ClusterCenter(`type`: String, coordinates: Array[Double])
    case class Properties(`marker-color`: String, title: String,
                          stddev: Double, fill: String,
                          `stroke-opacity`: Double, `fill-opacity`: Double)
    case class Feature(`type`: String, properties: Properties,
                       geometry: ClusterCenter)
    case class GjsonClusters(k: Int, silhouette: Double, `type`: String,
                             features: Array[Feature])

    var clustersArray: Array[Feature] = Array()

    // iterate clusters and build GeoJSON
    for ((vect, group) <- centers.zipWithIndex) {
      val lonLatVar = variancesDF.select(col("startLonVar"), col("startLatVar")).where(col("prediction") === group)
      val radiusStdDev: Double = math.sqrt((lonLatVar.first.getAs[Double](0) + lonLatVar.first.getAs[Double](1))/2)
      val coordinates: Array[Double] = Array(vect(0), vect(1))
      val tip = vect(2)
      val colorTip: Int = 255-(tip*alfa).toInt // substracted to switch red and white
      val colorTipString: String = f"$colorTip%02x" // Hex String
      val color = "ff" + colorTipString + colorTipString // construct a full color
      val dollarTip = beta*tip // rescaling
      clustersArray = clustersArray :+ Feature(
        `type`="Feature", properties=Properties(
          `marker-color`=color, s"Tip of $dollarTip", radiusStdDev, fill=("#"+color), `stroke-opacity`=0.2,`fill-opacity`=0.2
        ), geometry=ClusterCenter(`type`="Point", coordinates))
    }
    val gclusters = GjsonClusters(k, silhouette, `type`="FeatureCollection", features=clustersArray)
    gclusters.asJson
  }


}
