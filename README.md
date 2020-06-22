# Reddit Ads
Data pipeline for historical Reddit data

**Programming Languages**: Python, SQL, shell scripting
**Technologies**: Spark, Posgresql, Dash/Plotly
**AWS Services**: S3, EC2, Secrets Manager

## Introduction
Reddit, the self-proclaimed "front page of the internet", is the sixth most popular website in the US[Alexa](https://www.alexa.com/siteinfo/reddit.com#section_traffic "Reddit Alexa Ranking"), and has on average 21 billion screen views per month [Reddit-Stats](https://foundationinc.co/lab/reddit-statistics/ "Reddit Statistics for 2020"). This is prime real estate for advertising. Adverstisers want to maximize the reach of their advertisements, either by advertising early on posts that are engaging, or commenting on these posts early to maximize user interaction.

While the term "engaging post" is ambiguous and may be the subject of it own data science study, I defined "engaging post" as a post that has a greater than average number of comments or score. I purposely leave out a time window in that definition, which allows us to base the average on our time scale of interest. This allows advertisers the flexibility to define the threshold for an engaging post based on the appropriate time scale that best matches that of the advertising campaign.

## The Data
The data is the daily post data for December 2019 downloaded from [pushshift](https://files.pushshift.io/reddit/daily/). The data is stored in JSON format. The files are stored in an AWS.

## Data Processing
### Conversion to Parquet
In order to take advantage of parquets read-time optimizations, the data was converted from JSON to parquet format. The parquet file was uploaded to an AWS S3 bucket.

### Computing Statistics
All post data for a given day is stored in individual file. On a high level, Spark reads in the data from S3, computes the average values, and percentiles, and then writes the resulting data to postgres.
