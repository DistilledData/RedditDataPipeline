from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, avg, lit, to_date, unix_timestamp, percent_rank, from_utc_timestamp, from_unixtime, sum as _sum
from operator import add
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DateType, FloatType,  DoubleType
from pyspark.sql.window import Window
import time
import re

from get_postgres_credentials import get_secret

def compute_averages(sqlContext, df, secrets, post_date, logfile, table_name = "daily_post_averages"):
    '''
    Compute statistics for daily posts and write the information to a ddatabase

    @param df dataframe  that contains the daily post information
    @param secrets       the database connection credentials stored as a dict
    @param table_name    the name of the database table to which to write 
                         the aggregatred statistics
    '''
    #sum over columns to get total score, total number
    #comments and total number of posts
    averages_df = df.select("score", "num_comments").withColumn("count", lit(1)).agg(_sum("score").alias('total_score'), _sum("num_comments").alias('total_number_comments'), _sum("count").alias('post_count')).collect()
    averages_df1 = sqlContext.createDataFrame(averages_df)
    
    logfile.write("\t\tComputing averages\n")
    start = time.time()
    #compute averages from the sums
    #add the date as a column
    averages_df2 = averages_df1.withColumn("average_number_comments", averages_df1["total_number_comments"].cast('float')/averages_df1['post_count']).withColumn("average_score", averages_df1["total_score"].cast('float')/averages_df1['post_count']).withColumn('post_date',to_date(unix_timestamp(lit(post_date.replace("_","-")),'yyyy-MM-dd').cast("timestamp")))

    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))

    logfile.write("\tAdding average data to postgres\n")
    start = time.time()
    #{"username":"","password":"","engine":"","host":"","port":"","dbname":""}
    jdbc_url = 'jdbc:postgresql://' + secrets['host'] + ':' + secrets['port'] + '/' + secrets['dbname']
    averages_df2.write.jdbc(
        url=jdbc_url, 
        table = table_name, 
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

    logfile.write("\ttransforming post data\n")
    logfile.write("\t\tExtracting averages\n")
    start = time.time()
    #get average number of comments
    average_number_comments = averages_df2.select("average_number_comments").collect()[0]['average_number_comments']
    #get average score
    average_score = averages_df2.select("average_score").collect()[0]['average_score']
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))
    return average_score, average_number_comments


def compute_percentiles(df, secrets, average_score, average_number_comments,
                        logfile, daily_posts_table_name = "daily_posts"):

    logfile.write("\t\tCreating dataframe for postgres\n")
    start = time.time()
    #select subset of data and rename column
    daily_data_df = df.select("id", "url", "author", "created_utc", "subreddit", "title", "selftext", "num_comments", "score", "permalink").withColumnRenamed("id", "post_id").withColumnRenamed("selftext", "text").withColumnRenamed("num_comments", "number_comments")
    #convert post date from utc timestamp
    daily_data_df1 = daily_data_df.withColumn("post_date", from_unixtime(daily_data_df["created_utc"],'yyyy-MM-dd HH:mm:ss.SSS').cast("timestamp"))
    #add booleans whether the post is greater than the average score/number comments
    daily_data_df2 = daily_data_df1.withColumn("is_num_comments_greater_than_daily_average", daily_data_df["number_comments"] > average_number_comments).withColumn("is_score_greater_than_daily_average", daily_data_df["score"] > average_score)
    #do not include "created_utc" column
    daily_data_df3 = daily_data_df2.select("post_id", "permalink", "author", "post_date", "subreddit", "title", "text", "number_comments", "score", "is_score_greater_than_daily_average", "is_num_comments_greater_than_daily_average")
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))
    
    logfile.write("\t\tComputing percentiles for score\n")
    start = time.time()
    #compute percentile for score
    daily_data_df4 = daily_data_df3.withColumn("percentile_rank_score_daily", percent_rank().over(Window.partitionBy().orderBy(daily_data_df['score'])))
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))
    
    logfile.write("\t\tComputing percentiles for number comments\n")
    start = time.time()
    #compute percentile for number comments
    daily_data_df5 = daily_data_df4.withColumn("percentile_rank_number_comments_daily", percent_rank().over(Window.partitionBy().orderBy(daily_data_df['number_comments'])))
    end = time.time()
    logfile.write("\t\ttime elapsed: {} \n\n".format(end - start))

    logfile.write("\tAdding daily post data to db\n")
    start = time.time()
    jdbc_url = 'jdbc:postgresql://' + secrets['host'] + ':' + secrets['port'] + '/' + secrets['dbname']
    daily_data_df5.repartition(16).write.option("batchSize", 100000).jdbc(
        url=jdbc_url, 
        table = daily_posts_table_name, 
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


def process_daily_post_parquet_file(sc, sqlContext, post_year, post_month,
                                    post_day, file_path, logfile,
                                    averages_table_name = "daily_post_averages",
                                    daily_posts_table_name = "daily_posts",
                                    s3_bucket = "s3a://reddit-data-parquet/"):
    '''
    Compute averages and percentiles for the daily posts for the given year and month.

    @param sc spark                context instance
    @param sqlContext              SQLContext instance
    @param post_year               post year (e.g. 2019)
    @param post_month              post month (1 for January and 12 for 
                                   December)
    @param post_day                post day (e.g. 1)
    @param file_path               the path to the file in the AWS S3 bucket
    @param logfile                 the file to which to write
    @param averages_table_name     name of the database table that 
                                   holds statistics for daily post information
    @param daily_posts_table_name  name of the database table that holds daily
                                   post information
    @param s3_bucket               the s3 bucket where the parquet files exist
    '''
    
    post_date = str(post_year) + '-' + str(post_month) + '-' + str(post_day)
    
    logfile.write("\tReading in parquet file\n")
    start = time.time()
    df = sqlContext.read.parquet(s3_bucket + file_path).cache()
    end = time.time()
    logfile.write("\ttime elapsed: {} \n\n".format(end - start))
    
    logfile.write("\tComputing average number of comments and score\n")
    logfile.write("\tComputing sum of total_score, num_comments and count\n")
    start = time.time()
    
    #get database credentials
    secrets = get_secret()

    average_score, average_number_comments = compute_averages(sqlContext, df, secrets, post_date, logfile, averages_table_name)
     
    compute_percentiles(df, secrets, average_score, average_number_comments,
                        logfile, daily_posts_table_name)
