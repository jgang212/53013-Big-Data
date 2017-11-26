import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object JoinFlightsToWeather {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Build Flights and Weather Table").setMaster("local[2]");
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val d = sqlContext.table("delays")
    val w = sqlContext.table("orcweathersummary")

    // Hive "JOIN" is inner
    val joined = d.join(w, (col("d.year") === col("w.year")) 
      && (col("d.month") === col("w.month")) && (col("d.day") === col("w.day"))
      && (col("d.origin_code") === col("w.station")), "inner")

    val faw = joined.select(joined("d.year").as("year"), joined("d.month").as("month"),
      joined("d.day").as("day"), joined("d.carrier").as("carrier"),
      joined("d.flight").as("flight"), joined("d.origin_name").as("origin_name"),
      joined("d.origin_city").as("origin_city"), joined("d.origin_code").as("origin_code"),
      joined("d.dep_delay").as("dep_delay"), joined("d.dest_name").as("dest_name"),
      joined("d.dest_city").as("dest_city"), joined("d.dest_code").as("dest_code"),
      joined("d.arr_delay").as("arr_delay"), joined("w.meantemperature").as("mean_temperature"),
      joined("w.meanvisibility").as("mean_visibility"), joined("w.meanwindspeed").as("mean_windspeed"),
      joined("w.fog").as("fog"), joined("w.rain").as("rain"),
      joined("w.snow").as("snow"), joined("w.hail").as("hail"),
      joined("w.thunder").as("thunder"), joined("w.tornado").as("tornado"),
      joined(when("w.fog" === True, "d.dep_delay").otherwise(0)).as("fog_delay"),
      joined(when("w.rain" === True, "d.dep_delay").otherwise(0)).as("rain_delay"),
      joined(when("w.snow" === True, "d.dep_delay").otherwise(0)).as("snow_delay"),
      joined(when("w.hail" === True, "d.dep_delay").otherwise(0)).as("hail_delay"),
      joined(when("w.thunder" === True, "d.dep_delay").otherwise(0)).as("thunder_delay"),
      joined(when("w.tornado" === True, "d.dep_delay").otherwise(0)).as("tornado_delay"),
      joined(when(("w.fog" === True) || ("w.rain" === True) || ("w.snow" === True)
        || ("w.hail" === True) || ("w.thunder" === True) || ("w.tornado" === True), 0).
        otherwise("d.dep_delay")).as("clear_delay"))

    faw.saveAsTable("flights_and_weather")
  }
}