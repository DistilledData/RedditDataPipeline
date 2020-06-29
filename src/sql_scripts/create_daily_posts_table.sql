--table that holds Reddit post data
\c redddit;

CREATE TABLE daily_posts (
       post_id						VARCHAR(30) PRIMARY KEY,
       permalink					VARCHAR(255),
       author						VARCHAR(255),
       post_date					TIMESTAMP,
       subreddit					VARCHAR(255),
       title						TEXT,
       text						TEXT,
       number_comments					BIGINT,
       score						BIGINT,
       is_score_greater_than_daily_average		BOOLEAN,
       is_num_comments_greater_than_daily_average 	BOOLEAN,
       percentile_rank_score_daily 			FLOAT,
       percentile_rank_number_comments_daily 		FLOAT
);
