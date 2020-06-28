from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import  time

def convert_to_parquet(sqlContext, file_path, logfile, json_s3_bucket, parquet_s3_bucket):
    logfile.write("\tbeginning to read in json file from s3\n")
    start = time.time()
    df = sqlContext.read.json(json_s3_bucket + file_path)
    end = time.time()
    logfile.write("\ttime to  complete operation: {} s\n\n".format(end - start))

    logfile.write("\twriting  out file as parquet to s\n")
    start = time.time()
    df.repartition(8).write.parquet(parquet_s3_bucket + file_path + ".parquet")
    end = time.time()
    logfile.write("\ttime to  complete operation: {} s\n\n".format(end - start))
