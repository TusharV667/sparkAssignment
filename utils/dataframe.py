import requests
import csv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

URL = "https://covid-193.p.rapidapi.com"
HEADERS = {
    "X-RapidAPI-Key": "db077ce09emsh085e48f82ac6e79p1923aajsn482228f926a4",
    "X-RapidAPI-Host": "covid-193.p.rapidapi.com"
}

def get_country_data():
    try:
        response = requests.get(f"{URL}/statistics", headers=HEADERS)
        response.raise_for_status()  # Raise exception for non-2xx status codes
        data = response.json().get('response')
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching country data: {e}")
        return None

def write_to_csv(filename='country_data.csv'):
    try:
        country_data = get_country_data()
        if not country_data:
            logging.error("No country data available")
            return

        with open(filename, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=['Country', 'New Cases', 'Active Cases', 'Critical Cases',
                                                      'Total Cases', 'Total Recovered', 'New Deaths', 'Total Deaths'])
            writer.writeheader()
            for data in country_data:
                cases = data.get('cases', {})
                deaths = data.get('deaths', {})
                writer.writerow({
                    'Country': data.get('country', ''),
                    'New Cases': cases.get('new', ''),
                    'Active Cases': cases.get('active', ''),
                    'Critical Cases': cases.get('critical', ''),
                    'Total Cases': cases.get('total', ''),
                    'Total Recovered': cases.get('recovered', ''),
                    'New Deaths': deaths.get('new', ''),
                    'Total Deaths': deaths.get('total', '')
                })
        logging.info(f"Country data written to {filename}")
    except Exception as e:
        logging.error(f"Error writing to CSV: {e}")

def read_csv_to_spark(filename='./country_data.csv'):
    try:
        spark = SparkSession.builder.appName("COVID-19 Data").getOrCreate()
        schema = StructType([
            StructField("Country", StringType(), True),
            StructField("New Cases", IntegerType(), True),
            StructField("Active Cases", IntegerType(), True),
            StructField("Critical Cases", IntegerType(), True),
            StructField("Total Cases", IntegerType(), True),
            StructField("Total Recovered", IntegerType(), True),
            StructField("New Deaths", IntegerType(), True),
            StructField("Total Deaths", IntegerType(), True)
        ])
        df = spark.read.csv(filename, header=True, schema=schema)
        logging.info("Data loaded into dataframe")
        return df
    except Exception as e:
        logging.error(f"Error loading data into Spark dataframe: {e}")