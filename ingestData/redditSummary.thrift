namespace java edu.uchicago.mpcs53013.redditSummary

struct RedditSummary {
	1: required i64 gilded;
	2: required i64 num_comments;
	3: required i64 score;
	4: required string selftext;
	5: required string subreddit;
	6: required string title;
}