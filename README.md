My final project takes in a months worth (~7 GB) of Reddit post data,
and returns some stats for each subreddit in the data set.

First I ingested the data using thrift and all of the source files
for this are in the **ingestData/** directory. *redditSummary.thrift*
is the lists out the fields I'll be using (I didn't end up using 
title and selftext) from the raw file. In *RedditSummaryProcessor.java*,
I imported org.json.simple to work with the raw JSON data in lines 70
to 86. *SerializeRedditSummary.java* then writes the data to thrift (I 
split the original raw file into 10 chunks, denoted by the parts in 
line 47).

After ingesting the data to thrift, I wrote this data to HBase with
the files in the **batch/** directory. *reddit_thrift_to_orc.hql* first
converts the thrift data to orc format. Then *subreddit_stats.hql* does
the batching and sums up number of posts, gilded, comments, and score
grouped by subreddit. Lastly, *write_to_hbase.hql* writes this batched
data to HBase.

I then deployed this data as a webapp on the cluster. The URL is:
**http://35.184.157.197/jgang212/reddit-stats.html** You can enter any
existing subreddit (time of data is February 2015) and the app will
return the number of posts, average comments per post, average Reddit
gold per post, and average score per post for the given subreddit. In
the **webapp/** directory, *reddit-stats.html* and *subreddit-stats.pl*
create this page.

The speed layer is also implemented as a webapp at the URL:
**http://35.184.157.197/jgang212/submit-reddit.html** For a given post in
a subreddit, you can submit the number of comments, number of Reddit
gold given, and the score on the post. In the **webapp/** directory,
*submit-reddit.html* and *submit-reddit.pl* create this page. In the
**sparkStream/** directory, *StreamPosts.scala* uses *SubredditPost.scala*
to add the submitted data to the subreddit-post HBase table when the
speed layer is running.