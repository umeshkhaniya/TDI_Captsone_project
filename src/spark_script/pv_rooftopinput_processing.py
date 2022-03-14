import sys, os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

input_file_rooftop_albany_ny = "s3://data-est2-cap/rooftop/data_input_ny_albany/"
output_file_tosave  = 's3://data-est2-cap/rooftop/data_output_rooftop/data_output_rooftop'

# This is to convert the the_geom_4326 to Geohash.
def Geom4326ToGeo(the_geom_4326, precision=4):
    """transfer the the_geom_4326 to Geohash
    """
    geometry=shapely.wkt.loads(the_geom_4326)
    if geometry.geom_type == 'MultiPolygon':    # for MultiPolygon type geometry
        latitude,longitude=geometry.bounds[1], geometry.bounds[0]
    else:
                                            #find the centroid of geometry
        latitude, longitude= geometry.centroid.y, geometry.centroid.x
    res=pgh.encode(latitude,longitude, precision=precision)
    return res

if __name__ == "__main__":
    spark = (SparkSession
        .builder
        .appName('captsone')
        .getOrCreate())

    rooftop_input = spark.read.parquet(input_file_rooftop_albany_ny)
    df1 = rooftop_input.select(["the_geom_4326", "state", "city", "flatarea_m2", "slopearea_m2"])
    df_total = df1.withColumn("total_area_m2", F.col("flatarea_m2") + F.col("slopearea_m2"))
    df_total = df1.withColumn("total_area_m2", F.round(F.col("flatarea_m2") + F.col("slopearea_m2"), 2))
    df_total = df_total.select(["the_geom_4326", "state", "city", "total_area_m2"])
    udf_geohash = F.udf(lambda z: Geom4326ToGeo(z, precision=4))
    df_geohash = df_total.withColumn('goehash',udf_geohash("the_geom_4326"))
    df_geohash_final = df_geohash.groupBy("goehash", "state", "city").sum("total_area_m2").withColumnRenamed('sum(total_area_m2)', 'total_area_m2')
    df_geohash_final= df_geohash_final.withColumn("total_area_m2", F.round( "total_area_m2", 2))
    df_geohash_final.write.mode("overwrite").parquet(f"{output_file_tosave}.parquet")


	