package com.adaltas.taxistreaming.utils

object PointInPoly {
  def isPointInPoly(xCoordinate: Double, yCoordinate: Double, poly: Seq[Seq[Double]]): Boolean = {
    //https://wrf.ecse.rpi.edu/Research/Short_Notes/pnpoly.html
    val numberOfVertices = poly.length
    var i = 0
    var j = numberOfVertices-1
    var crossedPolygone = false
    for (i <- 0 until numberOfVertices) {
      if (( (poly(i)(1) > yCoordinate) != (poly(j)(1) > yCoordinate) ) &&
        ( xCoordinate < poly(i)(0) + (poly(j)(0) - poly(i)(0)) * (yCoordinate - poly(i)(1)) / (poly(j)(1) - poly(i)(1)) )
      ) crossedPolygone = !crossedPolygone
      j = i
    }
    crossedPolygone //true when point is in poly (vertical line crossed polygone an odd number of times)
  }
}