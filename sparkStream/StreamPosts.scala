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
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamPosts {
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
  val table = hbaseConnection.getTable(TableName.valueOf("subreddit-post"))
  val existingTable = hbaseConnection.getTable(TableName.valueOf("subreddit_stats_hbase"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamPosts <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamPosts")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set[String]("subreddit-post")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);

    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[SubredditPost]))

    // How to write to an HBase table
    val batchStats = reports.map(wr => {
      val put = new Put(Bytes.toBytes(wr.subreddit))
      put.addColumn(Bytes.toBytes("post"), Bytes.toBytes("comments"), Bytes.toBytes(wr.comments))
      put.addColumn(Bytes.toBytes("post"), Bytes.toBytes("golds"), Bytes.toBytes(wr.golds))
      put.addColumn(Bytes.toBytes("post"), Bytes.toBytes("score"), Bytes.toBytes(wr.score))
      table.put(put)
    })
    batchStats.print()
    
    val batchStats2 = reports.map(wr => {
      val increment = new Increment(Bytes.toBytes(wr.subreddit));
      increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("posts"), 1);
      increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("total_comments"), wr.comments);
      increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("total_gilded"), wr.golds);
      increment.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("total_score"), wr.score);
      existingTable.increment(increment)
      table.increment(increment)
    })
    batchStats2.print()
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
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}