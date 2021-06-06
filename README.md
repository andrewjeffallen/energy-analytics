#   Energy Analytics

Run analytics pipeline for [Utility-API](https://utilityapi.com/) data on AWS + Snowflake for use by energy engineers to analyze in [SkySpark](https://skyfoundry.com/) engine 

# Setup and Usage

## Prerequisites

Please have the following installed

1) `Python 3.9` or higher with the following libraries [MacOS installation guide here](https://formulae.brew.sh/formula/python@3.9)
- `numpy`
- `pandas`
- `boto3`
- `snowflake-connector-python`


To run install python libraries at once, run this in your terminal:

```
pip install -r src/requirements.txt 
```

2) git

If you are on MacOS, run this in your terminal:
```
$ brew install git
```

#### Step 1: Clone Repository

Once you have downloaded Python and Git onto your local machine, clone the `energy-analytics` repository. Run this in your terminal

```
$ git clone https://github.com/andrewjeffallen/energy-analytics.git
```

#### Step 2: Run data refresh

From within your `energy-analytics` directory, execute the following from your terminal:

```
$ python3 main.py
```

Success! Now you have successfully queried UtilityAPI.com endpoints and loaded data into Snowflake!

## Core Modules

### `utilityapi`

Connects to UtilityAPI endpoints, load json into gunzipped csv, loads to AWS S3 for object storage

### `snowflake`

Performs COPY INTO from S3 into Snowflake Database

### `aws`

common AWS utilities to access AWS API

### `weather` (WIP)

weather utilities data to integrate localized weather forecasts into energy analytics
