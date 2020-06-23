\c reddit;
COPY
(
	SELECT subreddit FROM daily_posts_2019_12_01
	UNION
	SELECT subreddit FROM daily_posts_2019_12_02
	UNION
	SELECT subreddit FROM daily_posts_2019_12_03
	UNION
	SELECT subreddit FROM daily_posts_2019_12_04
	UNION
	SELECT subreddit FROM daily_posts_2019_12_05
	UNION
	SELECT subreddit FROM daily_posts_2019_12_06
	UNION
	SELECT subreddit FROM daily_posts_2019_12_07
	UNION
	SELECT subreddit FROM daily_posts_2019_12_08
	UNION
	SELECT subreddit FROM daily_posts_2019_12_09
	UNION
	SELECT subreddit FROM daily_posts_2019_12_10
	UNION
	SELECT subreddit FROM daily_posts_2019_12_11
	UNION
	SELECT subreddit FROM daily_posts_2019_12_12
	UNION
	SELECT subreddit FROM daily_posts_2019_12_13
	UNION
	SELECT subreddit FROM daily_posts_2019_12_14
	UNION
	SELECT subreddit FROM daily_posts_2019_12_15
	UNION
	SELECT subreddit FROM daily_posts_2019_12_16
	UNION
	SELECT subreddit FROM daily_posts_2019_12_17
	UNION
	SELECT subreddit FROM daily_posts_2019_12_18
	UNION
	SELECT subreddit FROM daily_posts_2019_12_19
	UNION
	SELECT subreddit FROM daily_posts_2019_12_20
	UNION
	SELECT subreddit FROM daily_posts_2019_12_21
    	UNION
     	SELECT subreddit FROM daily_posts_2019_12_22
      	UNION
      	SELECT subreddit FROM daily_posts_2019_12_23
      	UNION
      	SELECT subreddit FROM daily_posts_2019_12_24
      	UNION
       	SELECT subreddit FROM daily_posts_2019_12_25
     	UNION
       	SELECT subreddit FROM daily_posts_2019_12_26
       	UNION
       	SELECT subreddit FROM daily_posts_2019_12_27
       	UNION
       	SELECT subreddit FROM daily_posts_2019_12_28
       	UNION
       	SELECT subreddit FROM daily_posts_2019_12_29
       	UNION
       	SELECT subreddit FROM daily_posts_2019_12_30
       	UNION
       	SELECT subreddit FROM daily_posts_2019_12_31
)
TO '/tmp/subreddit_list.csv' CSV HEADER;

