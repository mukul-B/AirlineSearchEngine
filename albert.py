from CreateDataframe import get_airports
def problem1(spark,Country="United States"):

    airlines = get_airports(spark)
    airlines.where("Country='" + str(Country)+"'").show(10)


#Mar 18, 2022
def problem5(spark):
    pass
#Mar 31, 2022
def problem7(spark):
    pass
#Apr 10, 2022