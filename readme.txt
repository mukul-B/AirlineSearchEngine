Airline Search Engine. You can use the data stored here: http://openflights.org/data.html.
This dataset contains the flight details of various airlines like:
Airport id, Name of the airport, Main city served by airport,
Country or territory where airport is located, Code of Airport, Decimal degrees, Hours offset from UTC,
 Timezone, etc.
Implement an airline data search engine using Hadoop, Spark, or other big data tools.

 The tool is able to help users to find out facts/trips with requested information/constraints:

- Airport and airline search:
Find list of Airports operating in the Country X
Find the list of Airlines having X stops
List of Airlines operating with code share
Find the list of Active Airlines in the United States

- Airline aggregation:
Which country (or) territory has the highest number of Airports
The top k cities with most incoming/outgoing airlines

- Trip recommendation:
Define a trip as a sequence of connected routes. Find a trip that connects two cities X and Y (reachability).
Find a trip that connects X and Y with less than Z stops (constrained reachability).
Find all the cities reachable within d hops of a city (bounded reachability).

Fast Transitive closure/connected component implemented in parallel/distributed algorithms.

set environment variable to run graphframe
PYSPARK_SUBMIT_ARGS=--packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 pyspark-shell