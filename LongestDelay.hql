select origin_name, dest_name, AVG(dep_delay) as all_delay
from flights_and_weather
group by origin_name, dest_name
order by all_delay desc
limit 1;