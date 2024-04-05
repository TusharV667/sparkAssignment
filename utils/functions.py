from pyspark.sql.functions import col, when

def clean_data(df):
    df = df.fillna({'New Cases': 0, 'Active Cases': 0, 'Total Deaths': 0, 'Total Recovered': 0, 'Critical Cases': 0})
    df = df.dropna(subset=['Country', 'Total Cases'])
    df = df.filter(col('Country') != 'All')
    
    return df

# ------------------------------------------------------------------------------------------------ #

def get_most_affected_country(df):
    df = df.dropna(subset=['Total Cases', 'Total Deaths'])
    df = df.withColumn('Death Ratio', when(col('Total Cases') != 0, col('Total Deaths') / col('Total Cases')).otherwise(None))
    most_affected_country_row = df.orderBy(col('Death Ratio').desc_nulls_last()).first()
    
    return most_affected_country_row

# ------------------------------------------------------------------------------------------------ #
def get_least_affected_country(df):
    df = df.dropna(subset=['Total Cases', 'Total Deaths'])
    df = df.withColumn('Death Ratio', when(col('Total Cases') != 0, col('Total Deaths') / col('Total Cases')).otherwise(None))   
    least_affected_country_row = df.orderBy(col('Death Ratio')).first()

    return least_affected_country_row

# ------------------------------------------------------------------------------------------------ #
def highest_case_country(df):
    
    highest_cases_country_row = df.orderBy(df['Total Cases'].desc()).first()
    return highest_cases_country_row

# ------------------------------------------------------------------------------------------------ #
def least_case_country(df):
    
    least_case_country_row = df.orderBy(df['Total Cases']).first()
    return least_case_country_row

# ------------------------------------------------------------------------------------------------ #
def total_cases(df):
    
    total_cases_value = df.agg({"Total Cases": "sum"}).first()[0]
    return total_cases_value

# ------------------------------------------------------------------------------------------------ #
def most_efficient_country(df):  
    df = df.dropna(subset=['Total Cases', 'Total Recovered'])

    df = df.withColumn('Efficiency Ratio', when(col('Total Cases') != 0, col('Total Recovered') / col('Total Cases')).otherwise(None))
    most_efficient_country_row = df.orderBy(col('Efficiency Ratio').desc_nulls_last()).first()
    
    return most_efficient_country_row

# ------------------------------------------------------------------------------------------------ #
def least_efficient_country(df):
    df = df.dropna(subset=['Total Cases', 'Total Recovered'])

    df = df.withColumn('Efficiency Ratio', when(col('Total Cases') != 0, col('Total Recovered') / col('Total Cases')).otherwise(None)) 
    least_efficient_country_row = df.orderBy(col('Efficiency Ratio').asc_nulls_last()).first()   
    return least_efficient_country_row

# ------------------------------------------------------------------------------------------------ #
def least_suffering_country(df):
    
    df = df.orderBy(col('Critical Cases'))
    least_suffering_country_row = df.first()
    
    return least_suffering_country_row

# ------------------------------------------------------------------------------------------------ #
def most_suffering_country(df):
    
    df = df.orderBy(col('Critical Cases').desc())
    most_suffering_country_row = df.first()
    
    return most_suffering_country_row
