# make sure you have the right and readacess to S3 bucket

import pyspark
from uszipcode import SearchEngine
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pygeohash as pgh

# read the file from s3 bucket
input_solar_s3 = "s3://data-est2-cap/solar/input_nrel/input-nrel.parquet/"
#S3 bucket output file save location
output_file = "s3://data-est2-cap/final_data/final_data"
# rooftop data
rooftop_albany_ny_s3 = "s3://data-est2-cap/rooftop/data_input_ny_albany/"

# convert latitude and longitude to hash map
def LatLongToGeohash(latitude,longitude, precision=6):
    """change latitude and longitude to geohash
    """
    res= pgh.encode(latitude,longitude, precision=precision)
    return res
    

if __name__ == "__main__":
	# let convert the latitude and longitude into 2 decimal places
	spark = (SparkSession
        .builder
        .appName('captsone')
        .getOrCreate())
	
	solar_input_ingest = spark.read.parquet(input_solar_s3)

	solar_input_ingest_1= (solar_input_ingest.withColumn("latitude", F.round( "latitude", 2))
                     .withColumn("longitude", F.round( "longitude", 2))
                      .groupBy("latitude", "longitude").sum("ghi")
                      .withColumnRenamed('sum(ghi)', 'ghi')
                      )
	solar_input_ingest = spark.read.parquet(input_solar_s3)
	udf_geohash_lat_lo = F.udf(lambda lat, long : LatLongToGeohash(lat, long, precision=6))
	solar_geohash_final = solar_input_ingest_1.withColumn('geohash',udf_geohash_lat_lo('latitude', 'longitude'))
	rooftop_albany_ingest = spark.read.parquet("rooftop_albany_ny_s3")

	# join the data set
	final_roof_solar = (rooftop_albany_ingest.join(solar_geohash_final,solar_geohash_final.geohash ==  rooftop_albany_ingest.geohash,"inner")
            .withColumn("power_MWH", F.col("ghi") * F.col("total_area_m2")/ 10**6)
            .select(["latitude", "longitude", "ghi", "city", "power_MWH", "total_area_m2"])
            .withColumn("power_MWH", F.round( "power_MWH", 2))
            )

	final_roof_solar.write.mode("overwrite").parquet(f"{output_file}.parquet")













