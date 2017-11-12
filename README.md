7-1 with Extra Credit

For Spark, I modified the original DelaysWithSD.scala and removed
the weather filtering. Instead, I just added the "all_avg" column
and grouped the data only on route. Then, I sorted the entire table
and called "show()" on the first row to get the route with the
longest average delay. In this case, it was CAK->TYS, with an
average delay of 1236 minutes (there was only one flight and it had
a delay of 1236, so not the best data set).

In Hive, the simple query is in LongestDelay.hql. It follows the same
logic as above, where I group the data on route and selected the
average delay for each route. Like above, I sorted the table with the
highest average delay first and selected the first row. Again, it
returned CAK->TYS with an average delay of 1236 minutes.

7-2

Again, using the original DelaysWithSD.scala, I created a new
DelayByAirline.scala that adds "carrier" to the grouping in addition
to the route. I then sorted the resulting table by route and then by
delay. This now returns a table grouped by routes, with the highest
delay carrier as the first row in each group. I then ran an "agg()"
function to show the first "carrier" and highest "all_avg" in each
group, and then converted the resulting table to an RDD. I then
saved this RDD as a text file in multiple parts, in the /FlightStats
folder.