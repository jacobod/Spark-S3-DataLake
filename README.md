# Data Lakes with Spark
### Building Analytics Tables for Sparkify using Spark

#### By Jacob Dodd

## 1) Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

Sparkify runs one of the fastest growing music streaming services in the Southern California, USA market. While the streaming app gathers usage data in the form of raw jsons stored in an AWS S3 bucket, this data is not currently in a format or database conducive for analysis.

The purpose of this database is to allow for analysts and other users to be able run fast, intuitive queries on song play analysis. This data lake was created using Spark and exported to Parquets to allow for rapid query response times using a popular framework, Spark (via Python/SQL). The main advantage of Spark is that this framework allows for analysis and processing of "big data" with faster processing time than HDFS combined with an API that is very similar to Pandas, but also allows you to use SQL if you are more comfortable with that.

Examples of the type of queries Sparkify would like to know include: when do listeners listen the most during the day, which arists are the most popular, and do paid listeners listen more than free-tier users?


## 2) State and justify your database schema design and ETL pipeline.

The database is organized in what is considered a STAR schema, with different 'dimension' or attribute tables (i.e. artists, songs) tied to a central "fact" table that represents a transaction important to the business (i.e. songplays). To access each table, use Spark or any other framework that reads in parquet files. Read each file in as a seperate dataframe, and/or create a view in your Spark Session with the command "df.createGlobalTempView(table_name)". This allows you to use either the dataframes API for data manipulation or the Spark.SQL API if that is your preference.

The star schema format ensures data integrity, as attribute fields do not appear in more than one table. This means that when data needs to be updated, it only needs to be updated in one place. 

This format also simplifies the user queries, for example to see all the users, you would only need to execute ("SELECT * FROM users") to grab the users from the song table, rather than doing ("SELECT DISTINCT user_id,first_name,last_name,level FROM songplays"), if the user data was included in the songplays table in a denormalized format.

#### ETL Pipeline

The pipeline works as so: 
    
    1) load data stored in JSON in S3, 
    2) clean and transform data, then seperate out for export
    3) export final tables to parquet.
    
Keeping everything in SQL/dataframes, and using python to manage queries and connect to the API provides a clean and reproducible workflow. The code used in this project functions as "Infrastructure as code"- if we need to set up the database again, we can easily view, edit, and repeat the ETL and database creation process.

#### Repo Info/How to Run

First, open the .cfg file and enter your AWS credentials. These will allow you to access the S3 bucket where all of the database files are stored.
Next, open the etl.py file, create an console editor, and run the whole file. It is now complete!

