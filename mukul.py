from CreateDataframe import get_airports

def problem2(spark):
    data = get_airports(spark)
    print("45")
    # airlines = get_airlines(spark)
    # activeUS = airlines.where("Country='United States' and Active='Y'")
    # usAirport = data.where("Country='United States'")
    # usAirport.join(activeUS, "Country").show(1)
    pass

#Mar 18, 2022

def problem6(spark):
    pass

#Mar 31, 2022

def problem8(spark):
    pass
def problem9(spark):
    pass
#Apr 10, 2022