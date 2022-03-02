from pyspark.sql import SparkSession

from CreateDataframe import get_airports, get_airlines


def active_us_airline(spark):
    data = get_airports(spark)
    print("45")
    # airlines = get_airlines(spark)
    # activeUS = airlines.where("Country='United States' and Active='Y'")
    # usAirport = data.where("Country='United States'")
    # usAirport.join(activeUS, "Country").show(1)


def passf():
    print("koi")
    pass


def problem(i):
    spark = SparkSession.builder.master("local[1]") \
        .appName("airport6") \
        .getOrCreate()
    switcher = {
        0: active_us_airline(spark),
        1: passf
    }
    func = switcher.get(i, lambda: 'Invalid')
    return func()


if __name__ == '__main__':
    prob = 1
    problem(prob)
