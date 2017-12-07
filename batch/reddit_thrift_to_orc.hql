add jar hdfs:///jgang/uber-ingestData-0.0.1-SNAPSHOT.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS RedditSummary
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES (
      'serialization.class' = 'edu.uchicago.mpcs53013.redditSummary.RedditSummary',
      'serialization.format' =  'org.apache.thrift.protocol.TBinaryProtocol')
  STORED AS SEQUENCEFILE 
  LOCATION '/inputs/redditData';

CREATE TABLE OrcRedditSummary (                                
   gilded bigint,                                                 
   num_comments bigint,                                               
   score bigint,                                               
   selftext string,
   subreddit string,
   title string) stored as orc;

insert overwrite table OrcRedditSummary select * from RedditSummary;
