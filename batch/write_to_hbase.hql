create external table subreddit_stats_hbase (
subreddit string, posts int,
total_gilded bigint,
total_comments bigint,
total_score bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stats:posts,stats:total_gilded,stats:total_comments,stats:total_score')
TBLPROPERTIES ('hbase.table.name' = 'subreddit_stats_hbase');

insert overwrite table subreddit_stats_hbase
select subreddit, posts,
total_gilded,
total_comments,
total_score from subreddit_stats;
