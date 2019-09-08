import configparser
from datetime import datetime
import os
from pathlib import Path
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,DateType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Default']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """Creates a spark session object"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Takes in json files for songs, and process them into 2 tables:
        - Artists
        - Songs
    Final datasets are stored as parquet files.
    """
    # read song data file
    df = spark.read.json(input_data)

    # create global view for querying
    df.createOrReplaceTempView("songs_raw")

    # extract columns to create songs table
    #songs_table = spark.sql("""
    #    SELECT song_id,
    #           title,
    #           arist_id,
    #           year,
    #           duration
    #    FROM songs_ext""")
    # or if python
    songs_table = df.select("song_id","title","artist_id",'year',"duration")
    print(songs_table.show(n=5))
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"/songs_table.parquet")
    
    #artist_id, name, location, lattitude, longitude
    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id,
               artist_name AS name,
               artist_location AS location,
               artist_latitude AS latitude,
               artist_longitude AS longitude
        FROM songs_raw""")
    artists_table = artists_table.drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data+"/artist_table.parquet")


def calc_timestamp(ts):
    """
    Converts a timetamp to string taking into account
    extra 3 digits on given timetamps
    """
    return datetime.utcfromtimestamp(int(str(ts)[:-3])).strftime('%Y-%m-%d %H:%M:%S')


def convert_datetime(ts):
    """
    Converts a timetamp to datetime taking into account
    extra 3 digits on given timetamps
    """
    return datetime.fromtimestamp(int(str(ts)[:-3]))


def process_log_data(spark, input_data, output_data):
    """
    Takes in json files (as string or list) and processes them into 3 tables:
        - Users
        - Time
        - Songplays

    Final datasets are stored as parquet files.
    """
    # get filepath to log data file
    log_data = input_data  #"s3://udacity-dend/log_data"

    # read log data file
    df = spark.read.json(log_data)
    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    # create global view for querying
    df.createOrReplaceTempView("logs")

    #user_id, first_name, last_name, gender, level
    # extract columns for users table
    users_table = spark.sql("""
        SELECT
            userId AS user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
        FROM logs""")
    # drop duplicates
    users_table = users_table.drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data+"/users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(calc_timestamp,StringType())
    df = df.withColumn("timestamp", get_timestamp(df["ts"]))

    # create datetime column from original timestamp column
    get_datetime = udf(convert_datetime,DateType())
    df = df.withColumn("ts_date", get_datetime(df["ts"]))

    # renew df view
    df.createOrReplaceTempView("logs")
    
    #start_time, hour, day, week, month, year, weekday
    # extract columns to create time table
    times = spark.sql("""SELECT ts_date FROM logs""")
    #drop duplicates
    time_table = times.drop_duplicates()
    time_table = time_table.select(
        col("ts_date").alias("start_time"),
        hour(col("ts_date")).alias("hour"),
        dayofmonth(col("ts_date")).alias("day"),
        weekofyear(col("ts_date")).alias("week"),
        month(col("ts_date")).alias('month'),
        year(col("ts_date")).alias('year'),
        date_format(col("ts_date"),"EEEE").alias("weekday")
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data+"/time_table.parquet")
    
    # join df to songs for songplays
    songplays_table = spark.sql(
        """
        SELECT 
        * 
        FROM logs l 
        INNER JOIN songs_raw s
        ON l.song = s.title AND l.length = s.duration AND l.artist = s.artist_name
        """)
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # extract columns from joined song and log datasets to create songplays table
    #songplays_table = df.join(
    #    song_df,
    #    left_on=['song', 'length', 'artist'],
    #    right_on=['title', 'duration', 'artist_name'],
    #    how='inner')

    # create index
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    # select needed cols and rename
    songplays_table = songplays_table.select(
        "songplay_id",
        col("ts_date").alias("start_time"),
        col("userId").alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        col("sessionId").alias("session_id"),
        "location",
        col("userAgent").alias("user_agent")
    )
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data+"/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "./Data/" #"s3a://udacity-dend/"
    output_data = "spark-wharehouse"
    # use a local example for testing
    process_song_data(spark, input_data+"song_data/A/A/A/TRAAAAW128F429D538.json", output_data)
    process_log_data(spark, input_data+"*events.json", output_data)


if __name__ == "__main__":
    main()
