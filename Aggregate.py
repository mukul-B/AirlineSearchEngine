import string

import pip

from CreateDataframe import get_airports, get_airlines, get_routes
from pyspark.sql.functions import desc, col
from graphframes import *

def countries_with_highest_airports(spark, stops):
    airports = get_airports(spark)
    airports.filter(airports.Type == 'airport') \
        .groupBy('Country') \
        .count().withColumnRenamed('count', 'Number of Airports') \
        .show()

def top_cities_with_airlines(spark, top=10):
    routes = get_routes(spark)
    airport = get_airports(spark)
    airportWithRouteS = airport.join(routes,  (
            routes.Destination_airport_ID == airport.id),
                                    "inner")
    airportWithRouteD = airport.join(routes, (routes.Source_airport_ID == airport.id) ,
                                    "inner")
    airportWithRoute= airportWithRouteS.union(airportWithRouteD)
    airportWithRoute.groupBy("city").count().sort(desc("count")).show(top)



