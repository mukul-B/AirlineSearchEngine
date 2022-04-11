import string

from pyspark.sql.functions import desc, col, concat, lit
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
    airportWithRouteS = airport.join(routes,  (
            routes.Destination_airport_ID == airport.id),
                                    "inner")
    airportWithRouteD = airport.join(routes, (routes.Source_airport_ID == airport.id) ,
                                    "inner")
    airportWithRoute= airportWithRouteS.union(airportWithRouteD)
    airportWithRoute.groupBy("city").count().sort(desc("count")).show(top)

def problem7(spark,s="LEB",d="PKE"):

    routes = get_routes(spark).select("Source_airport_ID","Destination_airport_ID","Airline")\
        .withColumnRenamed("Source_airport_ID","src").withColumnRenamed("Destination_airport_ID","dst")\
        .withColumnRenamed("Airline","relation")
    airport = get_airports(spark).select("id","IATA Code").withColumnRenamed("IATA Code","Name")
    graph = GraphFrame(airport, routes)
    graph.bfs(
        fromExpr="Name = '"+s+"'",
        toExpr="Name = '"+d+"'",
    ).show()


def problem8(spark,s="LEB",d="PKE",hops=4):

    routes = get_routes(spark).select("Source_airport_ID","Destination_airport_ID","Airline")\
        .withColumnRenamed("Source_airport_ID","src").withColumnRenamed("Destination_airport_ID","dst")\
        .withColumnRenamed("Airline","relation")
    airport = get_airports(spark).select("id","IATA Code").withColumnRenamed("IATA Code","Name")
    graph = GraphFrame(airport, routes)
    # graph.degrees.show()
    graph.bfs(
        fromExpr="Name = '"+s+"'",
        toExpr="Name = '"+d+"'",
        maxPathLength=hops,
    ).show()



# Apr 10, 2022
def problem9(spark,s="LEB",hops=3):

    routes = get_routes(spark).select("Source_airport_ID","Destination_airport_ID","Airline")\
        .withColumnRenamed("Source_airport_ID","src").withColumnRenamed("Destination_airport_ID","dst")\
        .withColumnRenamed("Airline","relation")
    airport = get_airports(spark).select("id","IATA Code").withColumnRenamed("IATA Code","Name")
    graph = GraphFrame(airport, routes)

    ch=list(string.ascii_lowercase)
    def character(a):
        return ch[a % 26] + str(int(a / 26))
    findString=""
    for a in range(hops):
        findString+="("+character(a)+")-["+character(a)+character(a+1)+"]->("+character(a+1)+")"
        if (a < hops-1):
            findString+="; "
    print(findString)
    # g2= graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)").where("a.Name='KEF'")
    g2 = graph.find(findString).where("a0.Name='KEF'")
    g2.show()


