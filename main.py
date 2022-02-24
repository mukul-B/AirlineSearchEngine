from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

if __name__ == '__main__':

    spark = SparkSession.builder.master("local[1]") \
        .appName("airport") \
        .getOrCreate()
        # .config("spark.driver.memory", "15g") \

    SCHEMA = StructType([StructField('Airport id', IntegerType()),
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
        .csv("airports.dat")

    data.show(10)