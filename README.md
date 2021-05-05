#  NightDog Energy

Run analytics pipeline for [Utility-API](https://utilityapi.com/) data on AWS + Snowflake

## Core Functionality

### utilityapi.py

Connects to UtilityAPI endpoints, load json into gunzipped csv, loads to AWS S3 for object storage

### snowflake.py

Performs COPY INTO from S3 into Snowflake Database

### aws.py

common AWS utilities to access AWS API

### weather.py (WIP)

weather utilities data to integrate localized weather forecasts into energy analytics
