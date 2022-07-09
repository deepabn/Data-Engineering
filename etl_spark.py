import configparser
import os
import logging
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from pyspark.sql.functions import udf, col, lit, year, month, upper, to_date
from pyspark.sql.functions import monotonically_increasing_id

# setup logging 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS configuration
config = configparser.ConfigParser()
config.read('capstone.cfg')
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
SOURCE_S3_BUCKET = config['S3']['SOURCE_S3_BUCKET']
DEST_S3_BUCKET = config['S3']['DEST_S3_BUCKET']
AWS_ACCESS_KEY_ID     = config.get('AWS', 'AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

# data processing functions
def create_spark_session():
#     spark = SparkSession.builder\
#         .config("spark.jars.packages",\
#                 "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
#         .enableHiveSupport().getOrCreate()
#     spark = SparkSession.builder.\
#                 config("spark.jars.repositories", "https://repos.spark-packages.org/").\
#                 config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
#                 enableHiveSupport().getOrCreate()

#     spark = SparkSession.builder\
#                     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
#                     .enableHiveSupport().getOrCreate()
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.2") \
        .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .getOrCreate()
    
    return spark

def SAS_to_date(date):
    if date is not None:
        return pd.to_timedelta(date, unit='D') + pd.Timestamp('1960-1-1')
SAS_to_date_udf = udf(SAS_to_date, DateType())

def rename_columns(table, new_columns):
    for original, new in zip(table.columns, new_columns):
        table = table.withColumnRenamed(original, new)
    return table

def process_immigration_data(spark, input_data, output_data):
    """Process immigration data to get fact_immigration, 
    dim_immi_personal and dim_immi_airline tables
        Arguments:
            spark {object}: SparkSession object
            input_data {object}: Source S3 endpoint
            output_data {object}: Target S3 endpoint
        Returns:
            None
    """

    logging.info("Start processing immigration")
    
    df = spark.read.format('com.github.saurfang.sas.spark').load('s3a://capstone-data-travel/immigration_sub/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
#     df = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    logging.info("Start processing fact_immigration")
    # extract columns to create fact_immigration table
    fact_immigration = df.select('cicid', 'i94yr', 'i94mon', 'i94port', 'i94addr',\
                                 'arrdate', 'depdate', 'i94mode', 'i94visa').distinct()\
                         .withColumn("immigration_id", monotonically_increasing_id())

    # data wrangling to match data model
    new_columns = ['cic_id', 'year', 'month', 'city_code', 'state_code',\
                   'arrive_date', 'departure_date', 'mode', 'visa']
    fact_immigration = rename_columns(fact_immigration, new_columns)

    fact_immigration = fact_immigration.withColumn('country', lit('United States'))
    fact_immigration = fact_immigration.withColumn('arrive_date', \
                                        SAS_to_date_udf(col('arrive_date')))
    fact_immigration = fact_immigration.withColumn('departure_date', \
                                        SAS_to_date_udf(col('departure_date')))

    # write fact_immigration table to parquet files partitioned by state and city
    fact_immigration.write.mode("overwrite").partitionBy('state_code')\
                    .parquet(path=output_data + 'fact_immigration')

    logging.info("Start processing dim_immi_personal")
    # extract columns to create dim_immi_personal table
    dim_immi_personal = df.select('cicid', 'i94cit', 'i94res',\
                                  'biryear', 'gender', 'insnum').distinct()\
                          .withColumn("immi_personal_id", monotonically_increasing_id())

    # data wrangling to match data model
    new_columns = ['cic_id', 'citizen_country', 'residence_country',\
                   'birth_year', 'gender', 'ins_num']
    dim_immi_personal = rename_columns(dim_immi_personal, new_columns)

    # write dim_immi_personal table to parquet files
    dim_immi_personal.write.mode("overwrite")\
                     .parquet(path=output_data + 'dim_immi_personal')

    logging.info("Start processing dim_immi_airline")
    # extract columns to create dim_immi_airline table
    dim_immi_airline = df.select('cicid', 'airline', 'admnum', 'fltno', 'visatype').distinct()\
                         .withColumn("immi_airline_id", monotonically_increasing_id())

    # data wrangling to match data model
    new_columns = ['cic_id', 'airline', 'admin_num', 'flight_number', 'visa_type']
    dim_immi_airline = rename_columns(dim_immi_airline, new_columns)

    # write dim_immi_airline table to parquet files
    dim_immi_airline.write.mode("overwrite")\
                    .parquet(path=output_data + 'dim_immi_airline')

def main():
    logging.info("Creating Spark Session")
    spark = create_spark_session()
    logging.info("Finished creating Spark Session")
    input_data = SOURCE_S3_BUCKET
    output_data = DEST_S3_BUCKET

    logging.info("Processing process_immigration_data function")
    process_immigration_data(spark, input_data, output_data)    
    logging.info("Data processing completed")

if __name__ == "__main__":
	main()