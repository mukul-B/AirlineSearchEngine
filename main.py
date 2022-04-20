from pyspark.sql import SparkSession

from Aggregate import *
from Search import *
from Trip import *
from transitive import transitive

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]") \
        .appName("airport6") \
        .getOrCreate()
    prob = 5

    if prob == 1:
        airport_in_country(spark, country = "Greenland")
    elif prob == 2:
        airline_with_stops(spark)
    elif prob == 3:
        airline_with_codeshare(spark)
    elif prob == 4:
        active_airline(spark)
    elif prob == 5:
        countries_with_highest_airports(spark, stops = 0)
    elif prob == 6:
        top_cities_with_airlines(spark)
    elif prob == 7:
        trip_from2to(spark,s="LEB",d="PKE")
    elif prob == 8:
        trip_to_within_stops(spark, s="LEB", d="PKE", hops=3)
    elif prob == 9:
        trip_from_possible_with_hops(spark)
    elif prob == 10:
        transitive(spark)
    else:
        print("invalid option")
