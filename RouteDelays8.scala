import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object RouteDelays8 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Build Route Delays Table").setMaster("local[2]");
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val faw = sqlContext.table("flights_and_weather")

    val f_fog = faw.filter("fog=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("fog_flights"), sum(col("dep_delay")).as("fog_delays"))
    val f_rain = faw.filter("rain=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("rain_flights"), sum(col("dep_delay")).as("rain_delays"))
    val f_snow = faw.filter("snow=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("snow_flights"), sum(col("dep_delay")).as("snow_delays"))
    val f_hail = faw.filter("hail=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("hail_flights"), sum(col("dep_delay")).as("hail_delays"))
    val f_thunder = faw.filter("thunder=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("thunder_flights"), sum(col("dep_delay")).as("thunder_delays"))
    val f_tornado = faw.filter("fog=true").groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("tornado_flights"), sum(col("dep_delay")).as("tornado_delays"))
    val f_clear = faw.filter("fog=false and rain=false and snow=false and hail=false and thunder=false and tornado=false").
                              groupBy("origin_name", "dest_name").
      agg(count(col("*")).as("clear_flights"), sum(col("dep_delay")).as("clear_delays"))

    val rd =
      f_fog.join(f_rain, Seq("origin_name", "dest_name"), "leftouter").
           join(f_snow, Seq("origin_name", "dest_name"), "leftouter").
           join(f_hail, Seq("origin_name", "dest_name"), "leftouter").
           join(f_thunder, Seq("origin_name", "dest_name"), "leftouter").
           join(f_tornado, Seq("origin_name", "dest_name"), "leftouter").
           join(f_clear, Seq("origin_name", "dest_name"), "leftouter")

    rd.saveAsTable("route_delays_8")
  }
}