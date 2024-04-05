# COVID-19 Data Dashboard

This is a Flask web application that provides a features for accessing COVID-19 stats of the data fetched from ```https://rapidapi.com/api-sports/api/covid-193```.
The project utilizes HTML, CSS, Python (Flask), and PySpark for data processing.

## Project Structure

```
project/
│
├── app.py
├── templates/
│   └── index.html
├── country_data.csv
└── utils/
    ├── dataframe.py
    └── functions.py
```

- `app.py`: Main Flask application file.
- `templates/`: Directory containing HTML templates.
- `country_data.csv`: CSV file containing COVID-19 data.
- `utils/`: Directory containing utility Python files.

## Setup and Installation

1. Clone this repository to your local machine:

    ```bash
    git clone github.com/TusharV667/sparkAssignment
    ```

2. Install the required Python libraries:

    ```bash
    pip install -r requirements.txt
    ```

3. Start the Flask application:

    ```bash
    python app.py
    ```
    or
   ```bash
    python3 app.py
    ```

5. Open your web browser and navigate to `http://localhost:5000` to access the dashboard.

## Usage

- Navigate through the GUI provided by the web application to access different COVID-19 data endpoints.
- Click on the links in the navigation menu to view specific data, such as the most affected country, least affected country, total cases, etc.

## License

This project is licensed under the [MIT License](LICENSE).

