from pyspark.sql import SparkSession

from ram import *
from albert import *
from mukul import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]") \
        .appName("airport6") \
        .getOrCreate()
    prob = 5
    if prob == 1:
        search_for = "Greenland"
        problem1(spark, search_for)
    elif prob == 2:
        problem2(spark)
    elif prob == 3:
        problem3(spark)
    elif prob == 4:
        problem4(spark)
    elif prob == 5:
        search_for = 0
        problem5(spark, search_for)
    elif prob == 6:
        problem6(spark)
    elif prob == 7:
        problem7(spark)
    elif prob == 8:
        problem8(spark)
    elif prob == 9:
        problem9(spark)
    elif prob == 10:
        problem10(spark)
    else:
        print("invalid option")
