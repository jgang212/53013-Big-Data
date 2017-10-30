5-1:
Updated versions of route_delays.hql, write_to_hbase.hql and route-report.pl
are checked in the repo. I used stddev_pop in route_delays.hql to add an 
additional column for the standard deviation. Then in write_to_hbase.hql, I
also write the additional standard deviation columns. Lastly, in
route-report.pl, I get the values of the extra columns and then added another
row to the table. I also added "AVG" and "STD" as a column in the table for
clarity.

Screenshot_2017-10-27_20-12-41.png and Screenshot_2017-10-27_20-13-41.png are
screenshots of the updated app running on my VM.

5-2:
After updating the files in problem 5-1 and re-running queries as necessary, I
used the following query in beeline to answer this question:

SELECT route, clear_std 
from weather_delays_by_route_new 
ORDER BY clear_std 
DESC LIMIT 1;

This query tells me the single route that has the single highest standard
deviation for each type of weather. The results are as follows:

Clear: ASE -> SLC 364.1 min
Fog: ATL -> FAR 139.4 min
Rain: ANC -> DFW 196.3 min
Snow: JAC -> JFK 331.9 min
Hail: MSP -> LEX 67.5 min
Thunder: MIA -> TUL 165.9 min
Tornado: IND -> CLT 10.0 min

It seems like ASE to SLC in clear weather has the highest standard deviation
of any weather with any route. However, we should take this with a grain of
salt as it only represents a dataset of 11 flights.

5-4:
printSquares.scala contains a function that calculates a square and then under
the function, I print them in tuple format.

fibonacci.scala contains a function that calculates the nth fibonacci number
using tail recursion. Then under the function, I print the first 10 fibonacci
numbers.
