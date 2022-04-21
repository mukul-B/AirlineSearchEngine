import string

from pyspark.sql.functions import desc, col, concat, lit
from graphframes import *

from CreateDataframe import get_airports, get_airlines, get_routes



def trip_from2to(spark, src="LEB", dst="PKE"):
    # vertices
    airports = get_airports(spark) \
        .select('IATA Code', 'City', 'Country') \
        .withColumnRenamed('id', 'Airport ID') \
        .withColumnRenamed('IATA Code', 'id')

    # edges
    routes = get_routes(spark) \
        .select('Source_airport', 'Destination_airport') \
        .withColumnRenamed('Source_airport', 'src') \
        .withColumnRenamed('Destination_airport', 'dst')

    graph = GraphFrame(airports, routes)
    graph.bfs(f"id='{src}'", f"id='{dst}'").distinct().show(truncate=False)

def trip_to_within_stops(spark, s="LEB", d="PKE", stop=4):

    routes = get_routes(spark).select("Source_airport_ID","Destination_airport_ID","Airline")\
        .withColumnRenamed("Source_airport_ID","src").withColumnRenamed("Destination_airport_ID","dst")\
        .withColumnRenamed("Airline","relation")
    airport = get_airports(spark).select("id","IATA Code").withColumnRenamed("IATA Code","Name")
    graph = GraphFrame(airport, routes)
    # graph.degrees.show()
    graph.bfs(
        fromExpr="Name = '"+s+"'",
        toExpr="Name = '"+d+"'",
        maxPathLength=stop,
    ).show()



# Apr 10, 2022
def trip_from_possible_with_hops(spark, s="LEB", hops=3):

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

