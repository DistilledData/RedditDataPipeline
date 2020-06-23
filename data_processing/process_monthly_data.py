from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.functions import col, avg, lit, to_date, unix_timestamp, percent_rank, from_utc_timestamp, from_unixtime, sum as _sum, year, month, max as _max, coalesce, broadcast, spark_partition_id, rank
from operator import add
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DateType, FloatType,  DoubleType
from pyspark.sql.window import Window
import time
import re
import argparse
from calendar import monthrange

from get_postgres_credentials import get_secret

def process_monthly_data(post_year, post_month, average_score, average_number_comments,s3_bucket, logfile, secrets, table_name = "monthly_posts"):
    '''
    Compute post statistics for a given month and store results in a database.

    @param post_year                     year of the given posts (e.g. 2019)
    @param post_month                    month of the given posts (1 for January 
                                         and 12 for December)
    @param average_score average         score for the given month
    @param average_number_comments       average number of comments for the given 
                                         month
    @param s3_bucket                     the url of the AWS S3 bucket (e.g 
                                         "s3a://my-bucket/")
    @param logfile                       the logfile to which to write information
    @param secrets                       the dictionary of database credentials
    @param table_name                    the name of the database table to which 
                                         to write the data
    '''

    #get number of days in month
    first_day_weekday, number_days_month = monthrange(post_year, post_month) 
    #list parquet files that we need to open
    parquet_files = ['posts/2019-12/RS_' + str(post_year) + '-'  + str(post_month) + '-' + 
                     str(day).zfill(2) + ".parquet" for day in
                     range(1, number_days_month + 1)]
    
    logfile.write("Reading in parquet files\n")
    start1 = time.time()
    data = []
    #combine all the data
    for parquet_file in parquet_files:
        logfile.write("\tReading in parquet file" + parquet_file + "\n")
        start  = time.time()
        post_date_temp = re.sub(r'.*_', '', parquet_file)
        post_date_temp_string = post_date_temp.replace("_","-")
        #select relevant fields
        rdd_temp = sqlContext.read.parquet(s3_bucket + parquet_file).select("id",
                                        "num_comments", "score")
        #rename columns
        rdd_temp1 = rdd_temp.withColumnRenamed("id",
                    "post_id").withColumnRenamed("num_comments", "number_comments")
        #add post date to rdd and convert to rdd
        rdd_temp2 = rdd_temp1.withColumn('post_date',
                    to_date(unix_timestamp(lit(post_date_temp_string),
                    'yyyy-MM-dd').cast("timestamp"))).rdd
        data.append(rdd_temp2)
        end = time.time()
        logfile.write("\ttime elapsed: {} \n\n".format(end - start))
        
    logfile.write("\tUnioning parquet files\n")
    start = time.time()
    #union all rdd and transform into a dataframe
    monthly_df = sqlContext.createDataFrame(sc.union(data)).cache()
    end = time.time()
    logfile.write("\ttime elapsed: {} \n\n".format(end - start))
    
    end1 = time.time()
    logfile.write("time elapsed: {} \n\n".format(end1 - start1))

    logfile.write("\t\tComputing percentiles for score\n")
    start = time.time()
    #compute percentiles for score
    monthly_df1 = monthly_df.withColumn("percent_rank_number_comments_monthly", percent_rank().over(Window.partitionBy().orderBy(monthly_df['number_comments'])))
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))
    
    logfile.write("\t\tComputing percentiles for number comments\n")
    start = time.time()
    #compute percentiles for number_comments
    monthly_df2 = monthly_df1.withColumn("percent_rank_score_monthly", percent_rank().over(Window.partitionBy().orderBy(monthly_df1['score'])))
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))

    logfile.write("\t\tComputing boolean whether score/number comments is greater than monthly average\n")
    start = time.time()
    monthly_df3 = monthly_df2.withColumn("is_num_comments_greater_than_monthly_average", monthly_df2["number_comments"] > average_number_comments)
    monthly_df4 = monthly_df3.withColumn("is_score_greater_than_monthly_average", monthly_df3["score"] > average_score)
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))
    
    logfile.write("\tAdding monthly post data to postgres\n")
    start = time.time()

    #select subset of fields that will be written to database
    monthly_df5 = monthly_df4.select("post_id", "is_num_comments_greater_than_monthly_average", "is_score_greater_than_monthly_average", "percent_rank_number_comments_monthly", "percent_rank_score_monthly")
    #secrets dict: {"username":","password":","host":","port":"}
    jdbc_url = 'jdbc:postgresql://' + secrets['host'] + ':' + secrets['port'] + '/' + secrets['dbname']
    #write data to database
    monthly_df5.write.option("numPartitions", 100).option("batchSize", 20000).jdbc(
        url=jdbc_url, 
        table=table_name, 
        mode = 'append',
        properties={
            "user":secrets['username'], 
            "password":secrets['password'], 
            "driver":"org.postgresql.Driver",
            "client_encoding":"utf8"
        }
    )
    end = time.time()
    logfile.write("\ttime elapsed: {} \n\n".format(end - start))


def compute_monthly_averages(post_year, post_month, s3_bucket, logfile, secrets,
                             daily_averages_table_name = "daily_post_averages",
                             monthly_averages_table_name = "monthly_post_averages"):
    '''
    Compute averages for the month based on daily post statistics

    @param post_year     year of the given posts (e.g. 2019)
    @param post_month    month of the given posts (1 for January and 12 for December)
    @param s3_bucket                    the url of the AWS S3 bucket (e.g "s3a://my-bucket/")
    @param logfile                      the logfile to which to write information
    @param secrets                      the dictionary of database credentials
    @param daily_averages_table_name    the name of the database table that holds daily statistics for posts
    @param daily_averages_table_name    the name of the database table to which to write the statistics
    '''

    
    logfile.write("\tGetting average data from postgres\n")
    start = time.time()
    #{"username":"","password":","engine":"","host":","port":","dbname":"}
    jdbc_url = 'jdbc:postgresql://' + secrets['host'] + ':' + secrets['port'] + '/' + secrets['dbname']
    #read 
    averages_df =  spark.read.jdbc(
        url = jdbc_url, 
        table = daily_averages_table_name,
        properties={
            "user":secrets['username'], 
            "password":secrets['password'], 
            "driver":"org.postgresql.Driver", 
            "client_encoding":"utf8"
        }
    )

    #get only posts for year and month of interest
    averages_df = averages_df.filter(month(col("post_date")) ==
                    int(post_month)).filter(year(col("post_date"))
                    == int(post_year))
    #compute total sums
    averages_df_sums = averages_df.agg(_sum("total_score"),
                            _sum("total_number_comments"),
                            _sum("post_count"))
    averages_df_sums1  = averages_df_sums.withColumnRenamed(
                           "sum(total_number_comments)",
                           "total_number_comments").withColumnRenamed(
                           "sum(total_score)","total_score"
                           ).withColumnRenamed("sum(post_count)", "post_count")
    end = time.time()
    logfile.write("\ttime elapsed: {} \n\n".format(end - start))

    logfile.write("\t\tComputing averages\n")
    start = time.time()
    #compute average number comments
    averages_df_sums2 = averages_df_sums1.withColumn("average_number_comments",
                        averages_df_sums1["total_number_comments"].cast('float')
                        / averages_df_sums1['post_count'])
    #compute average score
    averages_df_sums3 = averages_df_sums2.withColumn("average_score",
                        averages_df_sums2["total_score"].cast('float')
                        / averages_df_sums2['post_count'])
    #add month and year as a date (we will set day to "01" for convenience)
    averages_df_sums4 = averages_df_sums3.withColumn('post_date',
                        to_date(unix_timestamp(lit(str(post_year) + "-" + str(post_month) +
                        "-01" ),'yyyy-MM-dd').cast("timestamp")))
    
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))

    logfile.write("\tAdding monthly averages to postgres\n")
    start = time.time()
    #write the statistics to the database
    averages_df_sums4.write.jdbc(
        url = jdbc_url, 
        table = monthly_averages_table_name,
        mode = 'append',
        properties={
            "user":secrets['username'], 
            "password":secrets['password'], 
            "driver":"org.postgresql.Driver", 
            "client_encoding":"utf8"
        }
    )
    end = time.time()
    logfile.write("\ttime elapsed: {} \n\n".format(end - start))

    average_number_comments = averages_df_sums4.select("average_number_comments").collect()[0]['average_number_comments']
    #get average score
    average_score = averages_df_sums4.select("average_score").collect()[0]['average_score']
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))
    return average_score, average_number_comments

    
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    required = parser.add_argument_group('required arguments')
    required.add_argument("--year", help="post year (e.g. 2019)",
                          required=True)
    required.add_argument("--month", help="post month (e.g. 1 for January" +
                           " and 12 for December",  required=True)
    required.add_argument("--s3_bucket", help="url for the AWS S3 bucket. " +
                          "This program assumes that the user is using parquet" +
                          " files generated by the convert_to_parquet.py utility " +
                          "script. The naming convention for that script is " +
                          "<s3_bucket>/posts/<year>-<month>/<year>-<month>-<day>" +
                          ".parquet e.g. 's3a://my-bucket/2019-12/2019-12-01.parquet'",
                          required=True)
    optional = parser.add_argument_group("optional arguments")
    optional.add_argument("--logfile_name", help="name of the logfile to which to" +
                          " write", default="logs/process_montly_data_2019_12.log")
    optional.add_argument("--daily_averages_table_name", help  = "name of the database table holding daily statistics for posts", default = "daily_post_averages")
    optional.add_argument("--monthly_averages_table_name", help = "name of the database table hojlding monthly statistics for posts", default = "monthly_post_averages")
    
    args =  parser.parse_args()
    
    sc = SparkContext("spark://ec2-3-219-180-255.compute-1.amazonaws.com:7077",
                      "aggregate_monthly_data_not_distributed_partition_TRIAL_3")
    sqlContext = SQLContext(sc)
    #sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")
    spark = SparkSession(sc)
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    logfile_name = args.logfile_name
    with open(logfile_name, "a") as logfile:
        s3_bucket = args.s3_bucket
        post_year  = int(args.year)
        post_month = int(args.month)

        #get database credentials from AWS secrets manager
        secrets = get_secret()

        average_score, average_number_comments = compute_monthly_averages(post_year, post_month, s3_bucket, logfile, secrets,
                                                                          args.daily_averages_table_name,
                                                                          args.monthly_averages_table_name)
        
        process_monthly_data(post_year, post_month, average_score, average_number_comments, s3_bucket, logfile, secrets)
        #(post_year, post_month, average_score, average_number_comments,s3_bucket, logfile, secrets,
