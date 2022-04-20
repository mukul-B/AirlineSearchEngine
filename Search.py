from CreateDataframe import *


def airport_in_country(spark, country):
    airports = get_airports(spark)
    airports.filter(airports.Country == country).show()


def airline_with_stops(spark, stops=1):
    airlines = get_airlines(spark)
    routes = get_routes(spark)
    airlines.join(routes, "id").where("Stops=" + str(stops)).show(10)

def active_airline(spark):
     airlines = get_airlines(spark)
     activeUS = airlines.where("Country='United States' and Active='Y'").show(10)
     print(activeUS)

     #usAirport = airlines.where("Country='United States'").show(10)

def airline_with_codeshare(spark, top=10):
    routes = get_routes(spark)
    airlines = get_airlines(spark)
    airlineswithcodeshare = routes.join(airlines, (airlines.id == routes.id) | (
    airlines.id == routes.id), "inner")
    airlineswithcodeshare.filter(routes.Codeshare == "Y").show(top)
    #airlineswithcodeshare.groupBy("Codeshare","Name").count().sort(desc("count")).show(10)



