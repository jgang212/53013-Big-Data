create table route_delays (
  origin_name string, dest_name string, flights int,
  fog_flights int, fog_delays double, fog_std double,
  rain_flights int, rain_delays double, rain_std double,
  snow_flights int, snow_delays double, snow_std double,
  hail_flights int, hail_delays double, hail_std double,
  thunder_flights int, thunder_delays double, thunder_std double,
  tornado_flights int, tornado_delays double, tornado_std double,
  clear_flights int, clear_delays double, clear_std double);

insert overwrite table route_delays
  select origin_name, dest_name, count(1),
  count(if(fog, 1, null)), sum(fog_delay), stddev_pop(fog_delay),
  count(if(rain, 1, null)), sum(rain_delay), stddev_pop(rain_delay),
  count(if(snow, 1, null)), sum(snow_delay), stddev_pop(snow_delay),
  count(if(hail, 1, null)), sum(hail_delay), stddev_pop(hail_delay),
  count(if(thunder, 1, null)), sum(thunder_delay), stddev_pop(thunder_delay),
  count(if(tornado, 1, null)), sum(tornado_delay), stddev_pop(tornado_delay),
  count(if(!fog and !rain and !snow and !hail and !thunder and !tornado, 1, null)), sum(clear_delay), stddev_pop(clear_delay)
  from flights_and_weather
  group by origin_name, dest_name;