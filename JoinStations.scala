import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

object JoinStations {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Build Stations Table").setMaster("local[2]");
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val t = sqlContext.table("ontime")
    val so = sqlContext.table("stations")
    val sd = sqlContext.table("stations")

    // Hive "JOIN" is inner
    val joined = t.join(so, col("t.origin") === col("so.name"), "inner").
      join(sd, col("t.dest") === col("sd.name"), "inner")

    val delays = joined.select(joined("t.year").as("year"), joined("t.month").as("month"),
      joined("t.dayofmonth").as("day"), joined("t.carrier").as("carrier"),
      joined("t.flightnum").as("flight"), joined("t.origin").as("origin_name"),
      joined("t.origincityname").as("origin_city"), joined("so.code").as("origin_code"),
      joined("t.depdelay").as("dep_delay"), joined("t.dest").as("dest_name"),
      joined("t.destcityname").as("dest_city"), joined("sd.code").as("dest_code"),
      joined("t.arrdelay").as("arr_delay"))

    delays.saveAsTable("delays")
  }
}
