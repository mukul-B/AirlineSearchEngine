from CreateDataframe import get_airports, get_airlines, get_routes


def problem2(spark,stops = 1):

    airlines = get_airlines(spark)
    routes = get_routes(spark)
    airlines.join(routes, "id").where("Stops="+str(stops)).show(10)


#Mar 18, 2022

def problem6(spark):
    pass

#Mar 31, 2022

def problem8(spark):
    pass
def problem9(spark):
    pass
#Apr 10, 2022