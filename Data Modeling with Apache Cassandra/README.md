# Project: Data Modeling with Apache Cassandra

This project focuses on modeling song data and user activity data for a music streaming app called **Sparkify**. We are intereted in understanding what songs users are listening to. This is implemented by creating a **Apache Cassandra NoSQL database**. Below mentioned are the steps involved:
1. ETL pipeline for preprocessing raw dataset located in `event_data` and creating a new dataset
2. This new dataset (`event_datafile_new.csv`) is utilized for answering questions like:
	1.  Give the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
	2. Give only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
	3. Give every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
3. Three new Apache Cassandra tables are created for each of the above mentioned queries
4. Inserted appropriate data into these tables from `event_datafile_new.csv` dataset
5. Verified the data by leveraing CQL SELECT statements

## Project Structure

```
Data Modeling with Apache Cassandra
|____event_data					# Raw Dataset
| |____...events.csv
|____event_datafile_new.csv		# new file (created from event_data) used for analysis
|____Project_1B.ipynb	# ETL builder and Apache Cassandra query analysis
|____images		        	
| |____image_event_datafile_new.jpg # An overview of the preprocessed csv file
```

## Acknowledgement

The Project FAQs section provided by Udacity have been very helpful to understand how the sequence of columns should be in CREATE/INSERT statements. All FAQs were really helpful at various stages of implementing this project.


## Thank You
