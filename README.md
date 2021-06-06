#   Energy Analytics

Run analytics pipeline for [Utility-API](https://utilityapi.com/) data on AWS + Snowflake

# Setup and Usage

## How to run `energy-analytics` locally

From within your `energy-analytics` directory, execute the following from your terminal:

```
$ python3 main.py
```

## Prerequisites

Please have the following installed

Python 3.9 or higher with the following libraries

- `numpy`
- `pandas`
- `boto3`
- `snowflake-connector-python`

To run install python libraries at once, run this in your terminal:

```
pip install -r src/requirements.txt 
```

## Core Modules

### `utilityapi`

Connects to UtilityAPI endpoints, load json into gunzipped csv, loads to AWS S3 for object storage

### `snowflake`

Performs COPY INTO from S3 into Snowflake Database

### `aws`

common AWS utilities to access AWS API

### `weather` (WIP)

weather utilities data to integrate localized weather forecasts into energy analytics
