import json
import geojson
import shapely
from shapely.geometry import shape, GeometryCollection

with open("clusters-mean.geojson") as f:
  data = json.load(f)
  features = data["features"]

for clusterInd in range(len(features)-1):
    stddev = features[clusterInd]["properties"]["stddev"]
    lon, lat = features[clusterInd]["geometry"]["coordinates"][0], features[clusterInd]["geometry"]["coordinates"][1]
    center = shapely.geometry.point.Point(lon,lat)
    circle = center.buffer(stddev)  # Degrees Radius
    poly = shapely.geometry.mapping(circle)
    data["features"][clusterInd]["geometry"] = {"type": "GeometryCollection", "geometries": [features[clusterInd]["geometry"],poly]}

with open('clusters-mean-stddev.geojson', 'w') as f:
   geojson.dump(data, f)
