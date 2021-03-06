# udacity-dataengineering-project5

S3 and Redshift hosted data warehouse and Airflow ETL pipeline for song and user activity for a music streaming app.

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The purpose of this project is to create high grade, dynamic data pipelines that extracts their data from S3 and processes them in Redshift.

## Database Design

There are two datasets that reside in S3:

### Song Dataset

The song dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song.

### Log Dataset

The log dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

## Instructions

It is required to set up your own IAM role, IAM user, and Redshift cluster in AWS. Once this is complete, you will need to use Airflow's UI to configure your AWS credentials and connection to Redshift.

If it is a brand new Redshift cluster, you will have to include the <code>CreateTablesOperator</code> in <code>udac_example_dag.py</code> script in order to create the SQL tables.

Next, turn on the DAG in the Airflow UI. The pipeline will run automatically.
