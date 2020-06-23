--create table to hold statistics for daily posts aggregated on monthly level
--use reference to daily_posts table to avoid duplicating data
\c reddit;

CREATE TABLE monthly_posts (
       post_id						VARCHAR(30) REFERENCES daily_posts(post_id) PRIMARY KEY,
       post_date					DATE,
       is_score_greater_than_monthly_average		BOOLEAN,
       is_num_comments_greater_than_monthly_average 	BOOLEAN,
       percent_rank_score_monthly 			FLOAT,
       percent_rank_number_comments_monthly 		FLOAT
);
