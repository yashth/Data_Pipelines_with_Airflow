## Date created
Date : 19-07-2019

## Project Title
Data Pipelines With Apache Airflow

## Description
In this project, we will apply the concepts of **Data Pipelines** using Apcahe Airflow to create data models and schedule tasks.

A music streaming company, **Sparkify**, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is *Apache Airflow*.

As data engineer, we need to create an create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Also keeping in mind that data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

## Project Steps:
* Create DAGs for data modeling
* Copy the data to Amazon Redshift cluster from S3 using sub-dags 
* Run data quality checks

## Files used

### Python Files:
* main_dag.py -- execute main functions and call sub-dags
* stage_redshift.py -- upload main tables to redshift cluster
* load_fact.py -- upload fact tables to redshift cluster
* load_dimension.py -- upload tables to redshift cluster
* data_quality.py -- run data quality checks
* sql_queries.py -- run sql queries

## DAG Diagram

![Sowing the data pipelines directed acyclic graph](/DAG_Diagram.png "DAG Diagram")


## Credits
http://millionsongdataset.com

