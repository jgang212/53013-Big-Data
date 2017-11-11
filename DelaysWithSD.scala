import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object DelaysWithSD {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Build SD Table").setMaster("local[2]");
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val faw = sqlContext.table("flights_and_weather")

    val routes = faw.select("origin_name","dest_name").distinct()
    
    val f_all = faw.groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("all_flights"), sum(col("dep_delay")).as("all_delays"),
        avg(col("dep_delay")).as("all_avg"), stddev(col("dep_delay")).as("all_stddev"))
    
    /*
    val f_fog = faw.filter("fog=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("fog_flights"), sum(col("dep_delay")).as("fog_delays"),
        avg(col("dep_delay")).as("fog_avg"), stddev(col("dep_delay")).as("fog_stddev"))
    val f_rain = faw.filter("rain=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("rain_flights"), sum(col("dep_delay")).as("rain_delays"),
        avg(col("dep_delay")).as("rain_avg"), stddev(col("dep_delay")).as("rain_stddev"))
    val f_snow = faw.filter("snow=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("snow_flights"), sum(col("dep_delay")).as("snow_delays"),
        avg(col("dep_delay")).as("snow_avg"), stddev(col("dep_delay")).as("snow_stddev"))
    val f_hail = faw.filter("hail=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("hail_flights"), sum(col("dep_delay")).as("hail_delays"),
        avg(col("dep_delay")).as("hail_avg"), stddev(col("dep_delay")).as("hail_stddev"))
    val f_thunder = faw.filter("thunder=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("thunder_flights"), sum(col("dep_delay")).as("thunder_delays"),
        avg(col("dep_delay")).as("thunder_avg"), stddev(col("dep_delay")).as("thunder_stddev"))
    val f_tornado = faw.filter("fog=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("tornado_flights"), sum(col("dep_delay")).as("tornado_delays"),
        avg(col("dep_delay")).as("tornado_avg"), stddev(col("dep_delay")).as("tornado_stddev"))

    val f_clear = faw.filter("fog=false and rain=false and snow=false and hail=false and thunder=false and tornado=false").
                              groupBy("origin_name", "dest_name").
       agg(count(col("*")).as("clear_flights"), sum(col("dep_delay")).as("clear_delays"),
        avg(col("dep_delay")).as("clear_avg"), stddev(col("dep_delay")).as("clear_stddev"))
    
    val final_table =
      routes.join(f_fog, Seq("origin_name", "dest_name"), "leftouter").
           join(f_rain, Seq("origin_name", "dest_name"), "leftouter").
           join(f_snow, Seq("origin_name", "dest_name"), "leftouter").
           join(f_hail, Seq("origin_name", "dest_name"), "leftouter").
           join(f_thunder, Seq("origin_name", "dest_name"), "leftouter").
           join(f_tornado, Seq("origin_name", "dest_name"), "leftouter").
           join(f_clear, Seq("origin_name", "dest_name"), "leftouter")
    */

    f_all.sort(desc("all_avg")).limit(1).show()
    
    /*
    print(final_table.agg(max("fog_avg")))
    print(final_table.agg(max("rain_avg")).head)
    print(final_table.agg(max("snow_avg")).head)
    print(final_table.agg(max("hail_avg")).head)
    print(final_table.agg(max("thunder_avg")).head)
    print(final_table.agg(max("tornado_avg")).head)
    print(final_table.agg(max("clear_avg")).head)*/
  }
}