--create table that holds monthly statistics for posts 
\c reddit;

CREATE TABLE monthly_post_averages (
       post_date 		   DATE PRIMARY KEY,
       total_score 		   BIGINT,
       total_number_comments	   BIGINT,
       post_count 		   BIGINT,
       average_number_comments 	   FLOAT,
       average_score 		   FLOAT
);
