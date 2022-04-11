from CreateDataframe import *

def problem1(spark, country):
    airports = get_airports(spark)
    airports.filter(airports.Country == country).show()
    pass

#Mar 18, 2022
def problem5(spark, stops):
    airports = get_airports(spark)
    airports.filter(airports.Type == 'airport').groupBy('Country').count().show()
    pass

#Mar 31, 2022
def problem7(spark):
    pass
