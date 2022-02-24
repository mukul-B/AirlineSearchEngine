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
                         StructField('Code', StringType()),
                         StructField('Code2', StringType()),
                         StructField('Decimal Degree', DoubleType()),
                         StructField('Hours offset', DoubleType()),
                         StructField('dk1', IntegerType()),
                         StructField('dk2', IntegerType()),
                         StructField('Timezone', StringType()),
                         StructField('airport', StringType()),
                         StructField('airport2', StringType())])
    # Prepare training and test data.
    data = spark.read.schema(SCHEMA).option("header", False).option("delimiter", ",") \
        .csv("airports.dat")

    data.show(10)