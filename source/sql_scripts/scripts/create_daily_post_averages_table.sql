--table that holds average statistics for the posts on a given day
\c reddit;

CREATE TABLE daily_post_averages (
       post_date 		 DATE PRIMARY KEY,
       total_score 		 BIGINT,
       total_number_comments 	 BIGINT,
       post_count 		 BIGINT,
       average_number_comments 	 FLOAT,
       average_score 		 FLOAT
);
