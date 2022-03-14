# This is post processing of the solar data.

import os
import sys
import s3fs
import h5py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, DataFrame

s3_input_solar = 's3://nrel-pds-nsrdb/v3/nsrdb_2013.h5'
output_to_save_nrel = 's3://data-est2-cap/solar/input_nrel/input-nrel'


# This will read the .h5 file from bucket. The file size is 1.5 TB and do processing in same in the Parquet format.
def Hdf5ToParquet(filepath, output_path, spark):
    meta_cols=['latitude','longitude']
    ghi_cols=['ghi']
    meta='meta' # store the geospatial info
    ghi='ghi' # dataset name of ghi: store the ghi (irradiation) infor
    # read the file
    s3=s3fs.S3FileSystem(anon=False)
    _h5file= h5py.File(s3.open(filepath), "r")
    file_size=_h5file[meta].shape[0]
    scale_factor=_h5file[ghi].attrs['psm_scale_factor']
     #divide into chunk b/c file is too large 1.5 TB
    chunk_size = 10000
    n_chunk= file_size//chunk_size+1
    
    def readchunk(v):
        s3=s3fs.S3FileSystem(anon=False)
        _h5file= h5py.File(s3.open(filepath), "r")
        meta_chunk=_h5file[meta][v*chunk_size:(v+1)*chunk_size]
        ghi_chunk=_h5file[ghi][:,v*chunk_size:(v+1)*chunk_size].sum(axis=0)/scale_factor 
        def convertNptype(nl):  
            """ 
            converting np data type to python type 
            since spark rdd doesn't know numpy
            """
            res=getattr(nl, "tolist", lambda: value)()
            return res
        meta_l=  [convertNptype(meta_chunk[x]) for x in meta_cols] # only meta_cols
        ghi_l = convertNptype(ghi_chunk)
        agg_l= meta_l+[ghi_l]
        return list(map(list, zip(*agg_l)))
    
    df=(spark.sparkContext.parallelize(range(0,n_chunk))
        .flatMap(lambda v: readchunk(v))
        .toDF(meta_cols+ghi_cols))
  
    df.write.mode("overwrite").parquet(f"{output_path}.parquet")
    
if __name__ == "__main__":
	spark = (SparkSession
        .builder
        .appName('captsone')
        .getOrCreate())
	df5ToParquet(s3_input_solar, output_to_save_nrel)

    