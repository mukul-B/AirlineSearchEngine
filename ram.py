import string

import pip

from CreateDataframe import get_airports, get_airlines, get_routes
from pyspark.sql.functions import desc, col
from graphframes import *


def problem3(spark):
     airlines = get_airlines(spark)
     activeUS = airlines.where("Country='United States' and Active='Y'").show(10)
     print(activeUS)

     #usAirport = airlines.where("Country='United States'").show(10)
     pass

def problem4(spark, top=10):
    routes = get_routes(spark)
    airlines = get_airlines(spark)
    airlineswithcodeshare = routes.join(airlines, (airlines.id == routes.id) | (
    airlines.id == routes.id), "inner")
    airlineswithcodeshare.filter(routes.Codeshare == "Y").show(10)
    #airlineswithcodeshare.groupBy("Codeshare","Name").count().sort(desc("count")).show(10)
    pass


#Mar 31, 2022


def problem9(spark,s="LEB",hops=2):

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
    g2 = graph.find(findString).where("a0.Name='LAX'")
    g2.show()
