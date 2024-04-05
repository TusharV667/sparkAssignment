from flask import Flask, jsonify, render_template
from utils.dataframe import write_to_csv, read_csv_to_spark
from utils.functions import get_most_affected_country, get_least_affected_country, highest_case_country, least_case_country, clean_data
from utils.functions import total_cases, most_efficient_country, least_efficient_country, most_suffering_country, least_suffering_country
import os
app = Flask(__name__)


if not os.path.exists('./country_data.csv'):   
    write_to_csv()
df = read_csv_to_spark()
df = clean_data(df)

@app.route('/', methods=['GET'])
def home():
    return render_template('index.html')

# ------------------------------------------------------------------------------------------------ #
@app.route('/most_affected_country', methods=['GET'])
def get_most_affected():
    most_affected_country_row = get_most_affected_country(df)
    country = most_affected_country_row['Country']
    death_ratio = most_affected_country_row['Death Ratio']

    return jsonify({
        'Country': country,
        'Death Ratio': death_ratio
    })
    
# ------------------------------------------------------------------------------------------------ #

@app.route('/least_affected_country', methods=['GET'])
def get_least_affected():
    least_affected_country_row = get_least_affected_country(df)
    country = least_affected_country_row['Country']
    death_ratio = least_affected_country_row['Death Ratio']

    return jsonify({
        'Country': country,
        'Death Ratio': death_ratio
    })

# ------------------------------------------------------------------------------------------------ #

@app.route('/highest_case_country', methods=['GET'])
def get_highest():
    highest_cases_country_row = highest_case_country(df)
    country = highest_cases_country_row['Country']
    total_cases = highest_cases_country_row['Total Cases']

    return jsonify({
        'Country': country,
        'Total Cases': total_cases
    })
  
# ------------------------------------------------------------------------------------------------ #
  
@app.route('/lowest_case_country', methods=['GET'])
def get_lowest():
    lowest_cases_country_row = least_case_country(df)
    country = lowest_cases_country_row['Country']
    total_cases = lowest_cases_country_row['Total Cases']

    return jsonify({
        'Country': country,
        'Total Cases': total_cases
    })
 
# ------------------------------------------------------------------------------------------------ #
    
@app.route('/totalCases', methods=['GET'])
def get_total():
    total = total_cases(df)
    
    return jsonify({
        'Total Cases': total
    })

# ------------------------------------------------------------------------------------------------ #

@app.route('/most_efficient_country', methods=['GET'])
def most_efficient():
    efficient_country_row = most_efficient_country(df)
    country = efficient_country_row['Country']
    efficiency_ratio = efficient_country_row['Efficiency Ratio']

    return jsonify({
        'Country': country,
        'Efficiency Ratio': efficiency_ratio
    })

# ------------------------------------------------------------------------------------------------ #

@app.route('/least_efficient_country', methods=['GET'])
def least_efficient():
    efficient_country_row = least_efficient_country(df)
    country = efficient_country_row['Country']
    efficiency_ratio = efficient_country_row['Efficiency Ratio']

    return jsonify({
        'Country': country,
        'Efficiency Ratio': efficiency_ratio
    })
    
# ------------------------------------------------------------------------------------------------ #

@app.route('/least_suffering_country', methods=['GET'])
def least_suffering():
    least_suffering_country_row = least_suffering_country(df)
    country = least_suffering_country_row['Country']
    critical_cases = least_suffering_country_row['Critical Cases']

    # Return JSON response with the country with the least critical cases
    return jsonify({
        'Country': country,
        'Critical Cases': critical_cases
    })
    
# ------------------------------------------------------------------------------------------------ #

@app.route('/most_suffering_country', methods=['GET'])
def most_suffering():
    most_suffering_country_row = most_suffering_country(df)
    country = most_suffering_country_row['Country']
    critical_cases = most_suffering_country_row['Critical Cases']

    # Return JSON response with the country with the most critical cases
    return jsonify({
        'Country': country,
        'Critical Cases': critical_cases
    })
# ------------------------------------------------------------------------------------------------ #
if __name__ == "__main__":
    app.run(debug=True)



