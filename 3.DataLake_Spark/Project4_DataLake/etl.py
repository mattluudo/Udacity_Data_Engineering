import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date 
MILLISECONDS = 1000


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Creates a standard spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Reads JSON song data and outputs artists and songs parquet files.

    Keyword arguments:
    spark -- spark session
    input_data -- file path to root directory of input data
    output_data -- file path to root directory of output data
    """
    
    # get filepath to song data file
    song_data = input_data
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data
        ORDER BY song_id
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite")\
                 .partitionBy("year", "artist_id")\
                 .parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, 
                        artist_name AS name, 
                        artist_location AS location, 
                        artist_latitude AS latitude, 
                        artist_longitude AS longitude
        FROM song_data
        ORDER BY artist_id desc
    """)
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """Reads JSON log data and outputs users, time and songplays parquet files.

    Keyword arguments:
    spark -- spark session
    input_data -- file path to root directory of input data
    output_data -- file path to root directory of output data
    """
    
    # get filepath to log data file
    log_data = input_data

    # read log data file
    df = spark.read.json(input_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView("log_data")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT  DISTINCT userId    AS user_id, 
                         firstName AS first_name, 
                         lastName  AS last_name, 
                         gender, 
                         level
        FROM log_data
        ORDER BY last_name
    """)
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    extract_timestamp = udf(lambda x: datetime.fromtimestamp(x / MILLISECONDS), t.TimestampType())
    df = df.withColumn("timestamp", extract_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    # get_datetime = udf(lambda x: to_date(x / MILLISECONDS), t.TimestampType())
    # df = df.withColumn("start_time", get_datetime(df.ts))

    # extract columns to create time table
    df.createOrReplaceTempView("log_data")
    time_table = spark.sql("""
        SELECT DISTINCT  timestamp as start_time,
                         hour(timestamp) AS hour,
                         day(timestamp)  AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM log_data
        ORDER BY start_time
    """)


    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite")\
                    .partitionBy("year", "month")\
                    .parquet(output_data + "time/")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
       SELECT  year(log_data.timestamp) as year,
               month(log_data.timestamp) as month,
               log_data.timestamp as start_time, 
               log_data.userId AS user_id, 
               log_data.level, 
               song_data.song_id, 
               song_data.artist_id ,
               log_data.sessionId AS session_id, 
               log_data.location, 
               log_data.userAgent AS user_agent

       FROM log_data 
       INNER JOIN song_data 
       ON song_data.artist_name = log_data.artist 
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite")\
                        .partitionBy("year", "month")\
                        .parquet(output_data + "song_plays/")

def main():
    spark = create_spark_session()
    
    # Replace 'LOCAL' with 'AWS' for full s3 input data and output to s3
    input_song_data = config['LOCAL']['SONG_DATA_PATH']
    input_log_data = config['LOCAL']['LOG_DATA_PATH']
    output_data = config['LOCAL']['OUTPUT_DATA_PATH']
    
    process_song_data(spark, input_song_data, output_data)    
    process_log_data(spark, input_log_data, output_data)


if __name__ == "__main__":
    main()
