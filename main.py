from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

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
        .csv("airports.dat")
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
        .csv("airlines.dat")
    return data
if __name__ == '__main__':

    spark = SparkSession.builder.master("local[1]") \
        .appName("airport") \
        .getOrCreate()
        # .config("spark.driver.memory", "15g") \

    data= get_airports(spark)
    airlines = get_airlines(spark)
    activeUS= airlines.where("Country='United States' and Active='Y'")
    #
    usAirport=data.where("Country='United States'")
    usAirport.join(activeUS,"Country").show(1)