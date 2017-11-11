import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row

object DelayByAirline {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Build SD Table").setMaster("local[2]");
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val faw = sqlContext.table("flights_and_weather")
      
    val f_all = faw.groupBy("origin_name", "dest_name", "carrier").
      agg(count(col("*")).as("all_flights"), sum(col("dep_delay")).as("all_delays"),
        avg(col("dep_delay")).as("all_avg"), stddev(col("dep_delay")).as("all_stddev"))
    
    val worstAvgs = f_all.orderBy(asc("origin_name"), asc("dest_name"), desc("all_avg")).
      groupBy("origin_name", "dest_name").agg(first(col("carrier")).as("carrier"),
          max(col("all_avg")).as("worst_airline_avg"))
    val rows: RDD[Row] = worstAvgs.rdd
    
    val outputFile = args(0)
    rows.saveAsTextFile(outputFile)   
  }
}