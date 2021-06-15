## Introduction 

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

## Project Description

In this project, you'll apply what you've learned on data modeling with Postgres and build an ETL pipeline using Python. To complete the project, you will need to define fact and dimension tables for a star schema for a particular analytic focus, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

## Prerequites 

In order to run this project, the following need to be installed locally
- python3
- psycopg2
- pandas
- os
- glob

Additionally a Postgres database needs to be available and running locally. This can be configured in etl.py

## How to run 

To run this project, it is important that you run these steps in this order.

The first step is to run 'python create_tables.py'. This deletes and creates the tables that will be used in this project. 

The next step is to run 'python etl.py'. This reads and proceses the data files and loads them into the tables. 

## Description of Files

### Data (directory)

This directory contains json files that are used to populate the tables.

### create_tables.py

This is a python script that is used to delete and create the tables that are used in this project.

### etl.ipnyb

This is a Jyupter Notebook that was used as testing to create etl.py

### sql_queries.py

This is a python script with SQL queries called by etl.py. All queries are defined in this file. 

### test.ipynb 

This is a Jyupter Notebook used to test to make sure the data was properly loaded in the tables. 

## Database schema design

For this project, I used the star schema to create 1 fact table (songplays) and 4 dimension tables (songs, users, time, artists).

<img width="803" alt="Entity Relationship Diagram" src="https://user-images.githubusercontent.com/16965314/121825648-7c5bfb00-cc68-11eb-8705-e7b3b783d4cf.png">


