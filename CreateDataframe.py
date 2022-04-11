from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

dir = "resource/"


def get_airports(spark):
    SCHEMA = StructType([StructField('id', IntegerType()),
                         StructField('Name', StringType()),
                         StructField('city', StringType()),
                         StructField('Country', StringType()),
                         StructField('IATA Code', StringType()),
                         StructField('ICAO Code', StringType()),
                         StructField('Latitude', DoubleType()),
                         StructField('Longitude', DoubleType()),
                         StructField('Altitude', IntegerType()),
                         StructField('Timezone', IntegerType()),
                         StructField('DST', StringType()),
                         StructField('Tz', StringType()),
                         StructField('Type', StringType()),
                         StructField('Source', StringType())])
    # Prepare training and test data.
    data = spark.read.schema(SCHEMA).option("header", False).option("delimiter", ",") \
        .csv(dir+"airports.dat")
    return data


def get_airlines(spark):
    SCHEMA = StructType([StructField('id', IntegerType()),
                         StructField('Name', StringType()),
                         StructField('Alias', StringType()),
                         StructField('IATA', StringType()),
                         StructField('ICAO', StringType()),
                         StructField('Callsign', StringType()),
                         StructField('Country', StringType()),
                         StructField('Active', StringType())])
    # Prepare training and test data.
    data = spark.read.schema(SCHEMA).option("header", False).option("delimiter", ",") \
        .csv(dir+"airlines.dat")
    return data
def get_routes(spark):
    SCHEMA = StructType([StructField('Airline', StringType()),
                         StructField('id', IntegerType()),
                         StructField('Source_airport', StringType()),
                         StructField('Source_airport_ID', IntegerType()),
                         StructField('Destination_airport', StringType()),
                         StructField('Destination_airport_ID', IntegerType()),
                         StructField('Codeshare', StringType()),
                         StructField('Stops', IntegerType()),
                         StructField('Equipment', StringType())])
    # Prepare training and test data.
    data = spark.read.schema(SCHEMA).option("header", False).option("delimiter", ",") \
        .csv(dir+"routes.dat")
    return data