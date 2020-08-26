import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['IAM_USER']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['IAM_USER']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a SparkSession and returns it.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads and processes the json song data to create the songs_table and the artists_table,
    and saves them into parquet files.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[["song_id", "title", "duration", "year", "artist_id"]]

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist').parquet(output_data + 'song_table')

    # extract columns to create artists table
    artists_table = df[["artist_id", "artist_latitude", "artist_longitude", "artist_location", "artist_name"]]

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists_table')


def process_log_data(spark, input_data, output_data):
    """
    Loads and processes the json logs and songs data to create the users_table, the time_table and the song_plays table,
    and saves them into parquet files.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table
    users_table = df[['userId, firstName, lastName, gender, level']]

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'artists_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (x / 1000.0))
    df = df.withColumn("ts", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    df = df.withColumn("start_time", get_datetime(col("ts")))

    # extract columns to create time table
    get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
    get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
    get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
    get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
    get_week = udf(lambda x: datetime.fromtimestamp(x / 1000.0).isocalendar()[1])
    get_weekday = udf(lambda x: datetime.fromtimestamp(x / 1000.0).isocalendar()[2])

    df = df.withColumn("hour", get_hour(col("ts")))
    df = df.withColumn("day", get_day(col("ts")))
    df = df.withColumn("year", get_year(col("ts")))
    df = df.withColumn("month", get_month(col("ts")))
    df = df.withColumn("week", get_week(col("ts")))
    df = df.withColumn("weekday", get_weekday(col("ts")))

    time_table = df[["start_time", "hour", "day", 'week', 'month', 'year', 'weekday']]

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year', 'month').parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/")

    # extract columns from song datasets
    song_df = song_df['song_id', 'artist_id', 'duration', 'artist_name', 'title']

    # extract columns from log datasets
    log_dataset = df[['start_time', 'userId', 'level', 'sessionId', 'location', 'userAgent', 'artist', 'song', 'year',
                      'month', 'length']]

    # join the log and song tables to create the songplays table
    songplays_table = log_dataset.join(song_df,
                                       (log_dataset.artist == song_df.artist_name) & (log_dataset.song == song_df.title)
                                       & (log_dataset.length == song_df.duration), how='inner')
    # Select only final columns
    songplays_table = songplays_table [['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id',
                                        'location', 'user_agent', 'year', 'month']]

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.repartition('year', 'month')
    songplays_table.write.partitionBy('year', 'month').mode("overwrite").parquet(output_data + 'songplays_table')


def main():
    """
    Creates the SparkSession, loads and processes the json logs and songs data stored in s3
    to create the four dimension tables and the fact table, and save then in parquet file in s3.
    """
    spark = create_spark_session()

    # Settings needed to read data from s3
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", config['IAM_USER']['AWS_ACCESS_KEY_ID'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", config['IAM_USER']['AWS_SECRET_ACCESS_KEY'])
    spark.sparkContext._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.impl",
                                                      "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Settings needed to write data faster on s3
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-project-datalake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
