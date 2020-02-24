import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, FloatType as Fl, StringType as St, \
    IntegerType as In, ShortType as SInt, LongType as LInt, DoubleType as Dbl, TimestampType as Tst

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session
    :return: spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def create_song_schema():
    """
    Schema structure for song data
    :return: StructType
    """
    song_schema = R([
        Fld("num_songs", In()),
        Fld("artist_id", St()),
        Fld("artist_latitude", Fl()),
        Fld("artist_longitude", Fl()),
        Fld("artist_location", St()),
        Fld("artist_name", St()),
        Fld("song_id", St()),
        Fld("title", St()),
        Fld("duration", Fl()),
        Fld("year", SInt())
    ])
    return song_schema


def create_log_schema():
    """
    Schema structure for log data
    :return: StructType
    """
    log_schema = R(
        [
            Fld('artist', St()),
            Fld('auth', St()),
            Fld('firstName', St()),
            Fld('gender', St()),
            Fld('itemInSession', LInt()),
            Fld('lastName', St()),
            Fld('length', Fl()),
            Fld('level', St()),
            Fld('location', St()),
            Fld('method', St()),
            Fld('page', St()),
            Fld('registration', Dbl()),
            Fld('sessionId', LInt()),
            Fld('song', St()),
            Fld('status', LInt()),
            Fld('ts', LInt()),
            Fld('userAgent', St()),
            Fld('userId', St())
        ]
    )

    return log_schema


def process_song_data(spark, input_data, output_data):
    """
    This function processes song data and creates parquet columnar format for song and artist
    :param spark: spark session
    :param input_data: S3 path for input data
    :param output_data: S3 path for output data
    :return: None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/A/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data, schema=create_song_schema())

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).where(
        "song_id is not null").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, "song.parquet"), mode=None, partitionBy=["year", "artist_id"],
                              compression=None)

    # extract columns to create artists table
    artists_table = df.select(
        ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).where(
        "artist_id is not null").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artist.parquet"))


def process_log_data(spark, input_data, output_data):
    """
    This function processes log data and creates parquet columnar format for user, time and songplay data
    :param spark: spark session
    :param input_data: S3 path for input data
    :param output_data: S3 path for output data
    :return: None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data, schema=create_log_schema())

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).where(
        "userId is not null").dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "user.parquet"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), Tst())
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    df = df.withColumn("date_time", date_format("start_time", 'MM/dd/yyyy HH:mm:ss'))

    # add date time parts to the dataframe
    df = df.withColumn("hour", hour("start_time")). \
        withColumn("day", dayofmonth("start_time")). \
        withColumn("week", weekofyear("start_time")). \
        withColumn("month", month("start_time")). \
        withColumn("year", year("start_time")). \
        withColumn("weekday", dayofweek("start_time"))

    # extract columns to create time table
    time_table = df.select(["ts", "hour", "day", "week", "month", "year", "weekday"])

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time.parquet"), mode=None, partitionBy=["year", "month"],
                                  compression=None)

    # get song and artist data from the previously created parquet files
    df1 = spark.read.parquet(os.path.join(output_data, "song.parquet"))
    df2 = spark.read.parquet(os.path.join(output_data, "artist.parquet"))

    # read in song data to use for songplays table
    song_df = df1.join(df2, df1["artist_id"] == df2["artist_id"]).select(df1["artist_id"], df1["song_id"],
                                                                         df1["duration"], df2["artist_name"],
                                                                         df1["title"])

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (
                df.length == song_df.duration), how='left').select(
        ["ts", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "year", "month"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "songplay.parquet"), mode=None, partitionBy=["year", "month"],
                              compression=None)


def main():
    """
    Main method to call supporting data processing functions
    :return: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
