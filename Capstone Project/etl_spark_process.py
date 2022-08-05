# Do all imports and installs here
import configparser
import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, LongType, StringType
import datetime
from pyspark.sql.functions import date_add, when, lower, isnull, dayofmonth, hour, weekofyear, dayofweek, date_format, udf, col, lit, year, month, upper, to_date
from pyspark.sql.types import DateType
from datetime import timedelta, datetime
import logging

# setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Fetching the AWS credentials
config = configparser.ConfigParser()
config.read('aws_config.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get('AWS','AWS_SECRET_ACCESS_KEY')

# Creating a spark session
def create_spark_session():
    spark = SparkSession \
    .builder \
    .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.1.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0") \
    .enableHiveSupport() \
    .getOrCreate()
    return spark

def code_mapper(file, idx):
    """
    Function to parse I94_SAS_Labels_Descriptions.SAS file

    :param file: file contents
    :param idx: header names

    :return: dictionary of contents
    """
    f_content2 = file[file.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

def process_i94_immigration_data(spark):
    """
    Function to process immigration data to get fact_immigration_i94,
    dim_immigration_personal and dim__immigration_airline tables

    :param spark: Spark session object

    :return: None
    """

    # Staging "i94_apr16_sub.sas7bdat" dataset using SparkSession
    logging.info("Start processing i94_apr16_sub.sas7bdat dataset")
    df_raw_immigration = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    
    # Cleaning Steps for fact_immigration_i94 table
    # Selecting preferred columns for fact_immigration_i94 table and casting it to preferred datatype
    logging.info("Start processing fact_immigration_i94")
    fact_immigration_i94 = df_raw_immigration.select(col('cicid').cast(IntegerType()), 
                                                     col('i94yr').cast(IntegerType()), 
                                                     col('i94mon').cast(IntegerType()), 
                                                     col('i94port'), col('i94addr'),
                                                     col('arrdate').cast(IntegerType()),
                                                     col('depdate').cast(IntegerType()),
                                                     col('i94mode').cast(IntegerType()),
                                                     col('i94visa').cast(IntegerType())
                                                    )
    # Dropping duplicate records
    fact_immigration_i94 = fact_immigration_i94.dropDuplicates()

    # Changing the column names to an understandable format
    fact_immigration_i94 = fact_immigration_i94\
    .withColumnRenamed('cicid', 'cic_id')\
    .withColumnRenamed('i94yr', 'i94_year')\
    .withColumnRenamed('i94mon','i94_month')\
    .withColumnRenamed('i94port', 'city_code')\
    .withColumnRenamed('i94addr', 'state_code')\
    .withColumnRenamed('arrdate','arrival_date')\
    .withColumnRenamed('depdate', 'departure_date')\
    .withColumnRenamed('i94mode', 'i94_mode')\
    .withColumnRenamed('i94visa','i94_visa')

    # Function for converting SAS date format to "YYYY-mm-ddd" format
    date_format = "%Y-%m-%d"
    convert_sas_date_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))

    # Changing the "arrival_date" and "departure_date" SAS date columns to "YYYY-mm-dd" format
    fact_immigration_i94 = fact_immigration_i94.withColumn('arrival_date', convert_sas_date_udf(fact_immigration_i94.arrival_date))
    fact_immigration_i94 = fact_immigration_i94.withColumn('departure_date', convert_sas_date_udf(fact_immigration_i94.departure_date))

    # Adding new column "country" with literal "United States"
    fact_immigration_i94 = fact_immigration_i94.withColumn('country', lit('United States'))

    # Writing to parquet files partitioned by state
    logging.info("Writing to parquet files partitioned by state")
    fact_immigration_i94.write.parquet('fact_immigration_i94', partitionBy='state_code',mode="overwrite")
    logging.info("Finished writing to parquet files partitioned by state")

    # Cleaning Steps for dim_immigration_personal table
    # Selecting preferred column for dim_immigration_personal table and casting it to preferred datatype
    logging.info("Start processing dim_immigration_personal")
    dim_immigration_personal = df_raw_immigration.select(col('cicid').cast(IntegerType()), 
                                                         col('i94cit').cast(IntegerType()), 
                                                         col('i94res').cast(IntegerType()), 
                                                         col('biryear').cast(IntegerType()), 
                                                         col('gender')
                                                        )

    # Dropping duplicates
    dim_immigration_personal = dim_immigration_personal.dropDuplicates()

    # Changing the column names to an understandable format
    dim_immigration_personal = dim_immigration_personal\
    .withColumnRenamed('cicid', 'cic_id')\
    .withColumnRenamed('i94cit', 'citizen_of_country')\
    .withColumnRenamed('i94res','country_of_residence')\
    .withColumnRenamed('biryear', 'birth_year')\
    .withColumnRenamed('gender', 'gender')

    # Writing to parquet files partitioned by country_of_residence
    logging.info("Writing to parquet files partitioned by country_of_residence")
    dim_immigration_personal.write.parquet('dim_immigration_personal', partitionBy='country_of_residence', mode="overwrite")
    logging.info("Finished writing to parquet files")

    # Cleaning Steps for dim_immigration_airline table
    # Selecting preferred column for dim_immigration_airline table and casting it to preferred datatype
    logging.info("Start processing dim_immigration_airline")
    dim_immigration_airline = df_raw_immigration.select(col('cicid').cast(IntegerType()), 
                                                        col('airline'), 
                                                        col('admnum').cast(IntegerType()), 
                                                        col('fltno'),
                                                        col('visatype')
                                                       )

    # Dropping duplicates
    dim_immigration_airline = dim_immigration_airline.dropDuplicates()

    # Changing the column names to an understandable format
    dim_immigration_airline = dim_immigration_airline\
    .withColumnRenamed('cicid', 'cic_id')\
    .withColumnRenamed('airline', 'airline')\
    .withColumnRenamed('admnum','admin_num')\
    .withColumnRenamed('fltno', 'flight_num')\
    .withColumnRenamed('visatype', 'visa_type')

    # Writing to parquet files
    logging.info("Writing to parquet files")
    dim_immigration_airline.write.parquet('dim_immigration_airline', mode="overwrite")
    logging.info("Finished writing to parquet files")

def process_usa_temperature_data(spark):
    """ 
    Function to process temperature data to get dim_temperature table
    
    :param spark: Spark Session object

    :return: None
    """

    #Staging GlobalLandTemperaturesByCity.csv dataset using SparkSession
    logging.info("Start processing GlobalLandTemperaturesByCity.csv dataset")
    df_global_temperature = spark.read.format('csv').options(header='true', inferSchema='true').load("../../data2/GlobalLandTemperaturesByCity.csv")

    # Cleaning Steps for dim_temperature_usa table
    logging.info("Start processing dim_temperature_usa")
    # Filtering just the data related to 'United States'
    dim_temperature_usa = df_global_temperature.filter(df_global_temperature['country'] == 'United States')

    # Selecting preferred column for dim_temperature_usa table and casting it to preferred datatype
    dim_temperature_usa = dim_temperature_usa.select(to_date(col('dt')), 
                                                col('AverageTemperature').cast(DoubleType()), 
                                    col('AverageTemperatureUncertainty').cast(DoubleType()), 
                                                     col('City'), col('Country'),
                                                     col('Latitude'),
                                                     col('Longitude'))

    # Dropping duplicates
    dim_temperature_usa = dim_temperature_usa.dropDuplicates()

    # Changing the column names to an understandable format
    dim_temperature_usa = dim_temperature_usa\
    .withColumnRenamed('to_date(`dt`)', 'date')\
    .withColumnRenamed('AverageTemperature', 'avg_temp')\
    .withColumnRenamed('AverageTemperatureUncertainty','avg_temp_uncertainty')\
    .withColumnRenamed('City', 'city')\
    .withColumnRenamed('Country', 'country')\
    .withColumnRenamed('Latitude','latitude')\
    .withColumnRenamed('Longitude', 'longitude')

    # Creating an "year" column which be used for future querying by the BI team
    dim_temperature_usa = dim_temperature_usa.withColumn('year', year(dim_temperature_usa.date))

    # Writing to parquet files
    logging.info("Writing to parquet files")
    dim_temperature_usa.write.parquet('dim_temperature', mode="overwrite")
    logging.info("Finished writing to parquet files")

def process_SAS_label_descriptions_file(spark):
    """ 
    Function to parse SAS label desctiption data to get codes of country, state and city
    
    :param spark: Spark Session object

    :return: None
    """

    # Staging I94_SAS_Labels_Descriptions.SAS dataset
    logging.info("Start processing I94_SAS_Labels_Descriptions.SAS dataset")
    with open('I94_SAS_Labels_Descriptions.SAS') as f:
        f_content = f.read()
        f_content = f_content.replace('\t', '')

    # Creating Country dict
    logging.info("Creating Country dict")
    i94cit_res = code_mapper(f_content, "i94cntyl")

    # Creating City dict
    logging.info("Creating City dict")
    i94port = code_mapper(f_content, "i94prtl")

    # Creating State dict
    logging.info("Creating State dict")
    i94addr = code_mapper(f_content, "i94addrl")

    # Writing country_code to parquet files
    logging.info("Writing country_code to parquet files")
    spark.createDataFrame(i94cit_res.items(), ['code', 'country']).write.parquet('country_code', mode="overwrite")
    logging.info("Finished writing country_code to parquet file")

    # Writing city_code to parquet files
    logging.info("Writing city_code to parquet files")
    spark.createDataFrame(i94port.items(), ['code', 'city']).write.parquet('city_code', mode="overwrite")
    logging.info("Finished writing city_code to parquet file")

    # Writing state_code to parquet files
    logging.info("Writing state_code to parquet files")
    spark.createDataFrame(i94addr.items(), ['code', 'state']).write.parquet('state_code', mode="overwrite")
    logging.info("Finished writing state_code to parquet file")

def process_demographics_data(spark):
    """ 
    Function to process demographic data to create dim_demographics 
    
    :param spark {object}: Spark Session object
    :return: None
    """
    # Staging us-cities-demographics.csv dataset using SparkSession
    logging.info("Start processing us-cities-demographics.csv dataset")
    df_demographics = spark.read.csv("us-cities-demographics.csv", sep=';', header=True)

    # Cleaning Steps for df_demographics table
    # Selecting preferred column for dim_demographics table and casting it to preferred datatype
    logging.info("Start processing dim_demographics")
    dim_demographics = df_demographics.select(upper(col('City')), upper(col('State')),
                                              col('Male Population').cast(IntegerType()), 
                                              col('Female Population').cast(IntegerType()), 
                                              col('Number of Veterans').cast(IntegerType()), 
                                              col('Foreign-born').cast(IntegerType()),
                                              col('Race'),
                                              col('Total Population').cast(IntegerType()),
                                              col('Median Age').cast(DoubleType()),
                                              col('Average Household Size').cast(DoubleType()))

    # Dropping duplicates
    dim_demographics = dim_demographics.dropDuplicates()

    # Changing the column names to an understandable format
    dim_demographics = dim_demographics\
    .withColumnRenamed('upper(City)', 'city')\
    .withColumnRenamed('upper(State)', 'state')\
    .withColumnRenamed('Male Population','male_population')\
    .withColumnRenamed('Female Population', 'female_population')\
    .withColumnRenamed('Number of Veterans', 'num_of_vetarans')\
    .withColumnRenamed('Foreign-born','foreign_born')\
    .withColumnRenamed('Race', 'race')\
    .withColumnRenamed('Total Population', 'total_population')\
    .withColumnRenamed('Median Age','median_age')\
    .withColumnRenamed('Average Household Size', 'avg_household_size')

    # Writing dim_demographics to parquet files
    logging.info("Writing to parquet files")
    dim_demographics.write.parquet('dim_demographics',mode="overwrite")
    logging.info("Finished writing to parquet")

def main():
    logging.info("Creating Spark Session")
    spark = create_spark_session()

    logging.info("Processing process_i94_immigration_data function")
    process_i94_immigration_data(spark)

    logging.info("Processing process_SAS_label_descriptions_file function")
    process_SAS_label_descriptions_file(spark)

    logging.info("Processing process_usa_temperature_data function")
    process_usa_temperature_data(spark)

    logging.info("Processing process_demographics_data function")
    process_demographics_data(spark)

    logging.info("Data processing completed")
    
if __name__ == "__main__":
    main()