import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object StreamFlights {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  
  // Use the following two lines if you are building for the cluster 
  hbaseConf.set("hbase.zookeeper.quorum","mpcs530132017test-hgm1-1-20170924181440.c.mpcs53013-2017.internal,mpcs530132017test-hgm2-2-20170924181505.c.mpcs53013-2017.internal,mpcs530132017test-hgm3-3-20170924181529.c.mpcs53013-2017.internal")
  hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
  
  // Use the following line if you are building for the VM
  //hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("weather_delays_by_route_8"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamFlights")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set[String]("flights")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaFlightRecord]))

    // How to read from an HBase table
    val batchStats = kfrs.map(kfr => {
      val result = table.get(new Get(Bytes.toBytes(kfr.originName + kfr.destinationName)))
      if(result == null || result.getRow() == null)
        RouteStats(kfr.originName, kfr.destinationName)
      else
        RouteStats(kfr.originName, kfr.destinationName,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("clear_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("clear_delays"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("fog_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("fog_delays"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("rain_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("rain_delays"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("snow_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("snow_delays"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("hail_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("hail_delays"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("thunder_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("thunder_delays"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("tornado_flights"))).toLong,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("tornado_delays"))).toLong)
    })
    
    // Your homework is to get a speed layer working
    //
    // In addition to reading from HBase, you will likely want to
    // either insert into HBase or increment existing values in HBase
    // You can do these just like the above, but instead of using a
    // Get object, you use a Put or Increment objects as documented here:
    //
    // http://javadox.com/org.apache.hbase/hbase-client/1.1.2/org/apache/hadoop/hbase/client/Put.html
    // http://javadox.com/org.apache.hbase/hbase-client/1.1.2/org/apache/hadoop/hbase/client/Increment.html
    //
    // One nuisance is that you can only increment by a Long, so
    // I have rebuilt our tables with Longs instead of Doubles
    
    // For now, this just prints the batch data we looked up to the console.
    // You can drop this once you have written the appropriate HBase code
    batchStats.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}