from pyspark.sql.functions import desc

from CreateDataframe import get_airports, get_airlines, get_routes


def problem2(spark, stops=1):
    airlines = get_airlines(spark)
    routes = get_routes(spark)
    airlines.join(routes, "id").where("Stops=" + str(stops)).show(10)


# Mar 18, 2022

def problem6(spark, top=10):

    routes = get_routes(spark)
    airport = get_airports(spark)
    airportWithRoute = airport.join(routes, (routes.Source_airport_ID == airport.id) | (
                routes.Destination_airport_ID == airport.id),
                                    "inner")
    airportWithRoute.groupBy("city").count().sort(desc("count")).show(top)


# Mar 31, 2022

def problem8(spark):
    pass


def problem9(spark):
    pass
# Apr 10, 2022
