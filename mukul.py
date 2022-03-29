from pyspark.sql.functions import desc, col
from graphframes import *

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

def problem8(spark,s="LEB",d="PKE"):

    routes = get_routes(spark).select("Source_airport_ID","Destination_airport_ID","Airline")\
        .withColumnRenamed("Source_airport_ID","src").withColumnRenamed("Destination_airport_ID","dst")\
        .withColumnRenamed("Airline","relation")
    airport = get_airports(spark).select("id","IATA Code").withColumnRenamed("IATA Code","Name")
    graph = GraphFrame(airport, routes)
    # graph.degrees.show()
    graph.bfs(
        fromExpr="Name = '"+s+"'",
        toExpr="Name = '"+d+"'",
    ).show()
    # graph.bfs(
    #     fromExpr="Name = 'BET'",
    #     toExpr="Name = 'TLT'",
    # ).show()

# Apr 10, 2022
