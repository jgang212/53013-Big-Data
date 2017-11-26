9-1:

I rewrote three of the .hql files to reimplement the batch layer in Spark:

join_stations.hql -> JoinStations.scala

join_flights_to_weather.hql -> JoinFlightsToWeather.scala

route_delays_8.hql -> RouteDelays8.scala

The remaining .hql files populate the ontime and OrcWeatherSummary tables and
write to Hbase.