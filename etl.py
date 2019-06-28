import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data +"/*.json"    #"s3://udacity-dend/song_data/*.json"

    # read song data file
    df = spark.read.json(song_data)

    # create global view for querying
    df.createGlobalTempView("songs")

    # extract columns to create songs table
    songs_table = spark.select("""
        SELECT song_id,
               title,
               arist_id,
               year,
               duration
        FROM songs""")
    # or if python
    #songs_table = df.select(song_id,title,arist_id,year,duration)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data+"/songs_table.parquet")

    #artist_id, name, location, lattitude, longitude
    # extract columns to create artists table
    artists_table = spark.select("""
        SELECT artist_id,
               artist_name AS name,
               artist_location AS location,
               artist_lattitude AS lattitude,
               artist_longitude AS longitude
        FROM songs""")
    artists_table = artists_table.drop_duplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data+"/artist_table.parquet")

def get_timestamp(ts):
    return datetime.utcfromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')

def get_datetime(ts):
    return datetime.fromtimestamp(float(ts))

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data +"/*.json" #"s3://udacity-dend/log_data"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(page='NextSong')

    # create global view for querying
    df.createGlobalTempView("logs")

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
    get_timestamp = udf(get_timestamp,StringType())
    df = df.withColumn("timestamp", get_timestamp(df["ts"]))

    # create datetime column from original timestamp column
    get_datetime = udf(get_datetime,DateType())
    df = df.withColumn("ts_date", get_datetime(df["ts"]))

    # renew df view
    df.createGlobalTempView("logs")
    #start_time, hour, day, week, month, year, weekday
    # extract columns to create time table
    times = spark.sql("""SELECT ts_date FROM logs""")
    #drop duplicates
    time_table = times.drop_duplicates()
    time_table = time_table.select(
        "ts_date".alias("start_time"),
        hour("ts_date").alias("hour"),
        dayofmonth("ts_date").alias("day"),
        weekofyear("ts_date").alias("week"),
        month("ts_date").alias('month'),
        year("ts_date").alias('year'),
        date_format("ts_date","EEEE").alias("weekday")
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+"/time_table.parquet")

    # read in song data to use for songplays table
    song_df = "s3://udacity-dend/song_data/*.json"

    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(
        song_df,
        left_on=['song', 'length', 'artist'],
        right_on=['title', 'duration', 'artist_name'],
        how='inner')
    # create index
    songplays_table = songplays_table.withColumn("songplay_id", monotonicallyIncreasingId())
    # select needed cols and rename
    songplays_table = songplays_table.select(
        "songplay_id",
        "ts_date".alias("start_time"),
        "userId".alias("user_id"),
        "level",
        "song_id",
        "artist_id",
        "sessionId".alias("session_id"),
        "location",
        "userAgent".alias("user_agent")
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data+"/songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "data/" #"s3a://udacity-dend/"
    output_data = "Output"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
