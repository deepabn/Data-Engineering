import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']

def create_spark_session():
    """
    Creates a new Spark session with the specified configuration or retrieves 
    the existing spark session and update the configuration
    
    :return spark: Spark session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads the song_data from AWS S3 (input_data) and extracts the songs and artists tables
    and then loaded the processed data back to S3 (output_data)
    
    :param spark: Spark Session object
    :param input_data: Location (AWS S3 path) of songs metadata (song_data) JSON files
    :param output_data: Location (AWS S3 path) where dimensional tables will be stored in parquet format 
    """

    # Get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')
    
    # Read song data file
    print("Reading song_data JSON files from S3")
    df = spark.read.json(song_data)
    print("Reading song_data files completed")

    # Create temp view of song data for songplays table to join
    df.createOrReplaceTempView('song_data_view')
    
    # Extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    

    # Write songs table to parquet files partitioned by year and artist
    print("Writing Songs table to S3 after processing")
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id')\
                .parquet(path=output_data + 'songs')
    print("Completed writing Songs in parquet format")

    # Extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',\
                              'artist_latitude', 'artist_longitude').distinct()
    
    # Write artists table to parquet files
    print("Writing Artists table to S3 after processing")
    artists_table.write.mode('overwrite').parquet(path=output_data + 'artists')
    print("Completed writing Artists in parquet format")


def process_log_data(spark, input_data, output_data):
    """
    Loads the log_data from AWS S3 (input_data) and extracts the users, time, songplays tables
    and then loaded the processed data back to S3 (output_data)
    
    :param spark: Spark Session object
    :param input_data: Location (AWS S3 path) of log_data metadata (log_data) JSON files
    :param output_data: Location (AWS S3 path) where dimensional tables will be stored in parquet format             
    """

    # Get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # Read log data file
    print("Reading log_data JSON files from S3")
    df = spark.read.json(log_data)
    print("Reading log_data files completed")
    
    # Filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # Extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # Write users table to parquet files
    print("Writing Users table to S3 after processing")
    users_table.write.mode('overwrite').parquet(path = output_data +'users')
    print("Completed writing Users in parquet format")

    """
    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    """    
    
    # Extract columns to create time table
    df = df.withColumn('start_time', (df['ts']/1000).cast('timestamp'))
    df = df.withColumn('weekday', date_format(df['start_time'], 'E'))
    df = df.withColumn('year', year(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('hour', hour(df['start_time']))  
    time_table = df.select('start_time', 'hour', 'day', 'week', \
                           'month', 'year', 'weekday').distinct()
    
    # Write time table to parquet files partitioned by year and month
    print("Writing Time table to S3 after processing")
    time_table.write.mode("overwrite").partitionBy('year', 'month')\
                    .parquet(path = output_data + 'time')
    print("Completed writing Time in parquet format")

    # Read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM song_data_view")

    # Extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title)\
                              & (df.artist == song_df.artist_name)\
                              & (df.length == song_df.duration), "inner")\
                        .distinct()\
                        .select('start_time', 'userId', 'level', 'song_id',\
                               'artist_id', 'sessionId', 'location', 'userAgent',\
                               df['year'].alias('year'), df['month'].alias('month'))\
                        .withColumn('songplay_id', monotonically_increasing_id())

    # Write songplays table to parquet files partitioned by year and month
    print("Writing Songplays table to S3 after processing")
    songplays_table.write.mode("overwrite").partitionBy('year', 'month')\
                    .parquet(path = output_data + 'songplays')
    print("Completed writing Songplays in parquet format")


def main():
    spark = create_spark_session()
    input_data = SOURCE_S3_BUCKET
    output_data = DEST_S3_BUCKET
    
    print("\n")

    print("Processing song_data files")
    process_song_data(spark, input_data, output_data) 
    print("Processing song_data files completed\n")

    print("Processing log_data files")
    process_log_data(spark, input_data, output_data)
    print("Processing log_data files completed\n")


if __name__ == "__main__":
    main()
