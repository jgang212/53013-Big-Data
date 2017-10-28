create external table weather_delays_by_route_new (
  route string,
  clear_flights int, clear_delays double, clear_std double,
  fog_flights int, fog_delays double, fog_std double,
  rain_flights int, rain_delays double, rain_std double,
  snow_flights int, snow_delays double, snow_std double,
  hail_flights int, hail_delays double, hail_std double,
  thunder_flights int, thunder_delays double, thunder_std double,
  tornado_flights int, tornado_delays double, tornado_std double)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,delay:clear_flights,delay:clear_delays,delay:clear_std,delay:fog_flights,delay:fog_delays,delay:fog_std,delay:rain_flights,delay:rain_delays,delay:rain_std,delay:snow_flights,delay:snow_delays,delay:snow_std,delay:hail_flights,delay:hail_delays,delay:hail_std,delay:thunder_flights,delay:thunder_delays,delay:thunder_std,delay:tornado_flights,delay:tornado_delays,delay:tornado_std')
TBLPROPERTIES ('hbase.table.name' = 'weather_delays_by_route_new');


insert overwrite table weather_delays_by_route_new
select concat(origin_name,dest_name),
  clear_flights, clear_delays, clear_std,
  fog_flights, fog_delays, fog_std,
  rain_flights, rain_delays, rain_std,
  snow_flights, snow_delays, snow_std,
  hail_flights, hail_delays, hail_std,
  thunder_flights, thunder_delays, thunder_std,
  tornado_flights, tornado_delays, tornado_std
from route_delays;
