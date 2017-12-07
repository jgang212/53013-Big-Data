create table subreddit_stats (
subreddit string, posts int,
total_gilded bigint,
total_comments bigint,
total_score bigint) stored as orc;

insert overwrite table subreddit_stats
select subreddit, count(1),
sum(gilded),
sum(num_comments),
sum(score)
from orcredditsummary
group by subreddit;