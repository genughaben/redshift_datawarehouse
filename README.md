## Documentation for AWS Redshift ETL

### How to

In .aws enter your:
* AWS Key
* AWS Secret

It will be loaded in config.py.

Use Notebook "ETL - Execute and inspect.ipynb" to setup AWS resources, execute ETL and inspect the resulting DWH.
After "SETUP" and before "EXECUTE" make sure to update DWH_ENDPOINT, DWH_ROLE_ARN and HOST.


### Description

This programm uses AWS S3 and AWS Redshift.
Data from two S3 buckets LOG_DATA and SONG_DATA is first COPYed into two staging tables (staging_events, staging_songs).
From these two staging tables a star data schema is populated using SQL-ETL.
The final star schema consists of the following tables:
* songplays (fact table)
* users (dimension table)
* songs (dimension table)
* artists (dimension table)
* time (dimension table)

Its full description can be viewed in its respective CREATE TABLE statements in sql_queries.py.

### Structure

* .aws_template - create a copy .aws of this file and add your AWS credentials. .gitignore prevents unintended sharing.
* dwh.cfg - contains all necessary parameters to control the project.
* config.py - creates a config parser to make parameters of dwh.cfg easier availble. AWS credentials are automatically added from .aws.
* aws_manager.py - contains class Redshift cluster that is capable to setup and shutdown a aws cluster using configuration in dwh.cfg.
* sql_queries.py - contains all necessary SQL queries to conduct the ETL process.
* create_tables.py - when the cluster is setup, drop and create statements are executed as defined in sql_queries.py.
* etl.py - after table creation in create_tables.py, here copy and insert statements are executed as defined in sql_queries.py
* start_etl.py - contains a shorthand to execute create_tables.py and etl.py; it is used in the "ETL - Execute and inspect.ipynb"
* "ETL - Execute and inspect.ipynb" - Here all the magic happends. The Notebook is divided into sections:
  * SETUP    - Here the redshift cluster is setup. After is setup, make sure to update DWH_ENDPOINT, DWH_ROLE_ARN and HOST. 
  * EXECUTE  - Here tables are dropped, created, staging is filled and ETL to final tables in conducted.
  * INSPECT  - Here some basic counts for entries in all tables are gathered for basic inspection.
  * ANALYSIS - Here a couple of queries are displayed to show the systems capability
  * SHUTDOWN - Here the redshift cluster is shutdown after use.
  
### Sample result:

#### Weekdays with songplays

Query: 
```sql
%%sql
SELECT time.week_day, count(songplays.id) as count_songplays
FROM songplays JOIN time ON songplays.start_time = time.start_time
GROUP BY week_day
ORDER BY week_day;
```

Results:

| week_day | count_songplays |
|----------|-----------------|
| 0        | 58              |
| 1        | 207             |
| 2        | 191             |
| 3        | 250             |
| 4        | 160             |
| 5        | 188             |
| 6        | 90              |
