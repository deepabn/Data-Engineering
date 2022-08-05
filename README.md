# Project: Data Modeling with Postgres

This project focuse on modeling song data and user activity data for a music streaming app called **Sparkify**. We are intereted in understanding what songs users are listening to. This is implemented by creating a **Postgres relational database** and **ETL pipeline** to build up **Fact and Dimension tables** and insert data into these tables.

## Project Structure

```
Data Modeling with Postgres
|____data			# Dataset
| |____log_data
| | |____...
| |____song_data
| | |____...
|____etl.ipynb		        # developing ETL builder
|____test.ipynb		        # testing ETL builder
|____etl.py			        # ETL builder
|____sql_queries.py		    # ETL query helper functions
|____create_tables.py		# database/table creation script
```

## ETL Pipeline
### **etl.py**: ETL pipeline builder

1. `process_data`: Iterates and process through each files in log_data or song_data folders
2. `process_song_file`: Performs ETL on song_data to create the **songs** and **artists** dimensional tables
3. `process_log_file`: Performs ETL on log_data to create the **time** and **users** dimensional tables and **songplays** fact table

### **create_tables.py**: Creates **sparkifydb** database and Fact and Dimension table schema

1. `create_database`: Creates and connects to the **sparkifydb**
2. `drop_tables`: Drops each table using the queries in `drop_table_queries` list
3. `create_tables`: Creates each table using the queries in `create_table_queries` list

### **sql_queries.py**: Helper SQL query statements for `etl.py` and `create_tables.py`

1. `*_table_drop`
2. `*_table_create`
3. `*_table_insert`
4. `song_select`

## Database Schema
### Fact Table
```
songplays
	- songplay_id 	PRIMARY KEY
	- start_time 	REFERENCES time (start_time)
	- user_id	REFERENCES users (user_id)
	- level
	- song_id 	REFERENCES songs (song_id)
	- artist_id 	REFERENCES artists (artist_id)
	- session_id
	- location
	- user_agent
```

### Dimension Tables
```
users
	- user_id 	PRIMARY KEY
	- first_name
	- last_name
	- gender
	- level
```

```
songs
	- song_id 	PRIMARY KEY
	- title
	- artist_id REFERENCES artists(artist_id)
	- year
	- duration
```

```
artists
	- artist_id 	PRIMARY KEY
	- name
	- location
	- latitude
	- longitude
```

```
time
	- start_time 	PRIMARY KEY
	- hour
	- day
	- week
	- month
	- year
	- weekday
```

## Thank You
