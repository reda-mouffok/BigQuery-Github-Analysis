# BigQuery Github Analysis

This project aims to extract github public data from BigQuery and providing a temporal distribution of GitHub commits per language using PySpark.

## Requirement 

Before running the program, install :
- Python 
- PySpark
- Google Cloud Library  
  - Installation command :```shell pip install --upgrade google-cloud-bigquery```
- Matplotlib 

Enable Google Cloud BigQuery API on your GCP account, then create a Service Account for BigQuery API to grant permission to your Python script and export the key as json file.
After, put the json file in the credentials' folder at the root of the project.

## Configuration

**Spark :** you can add spark session configurations in the file  `/conf/app.properties`.

**Reading & saving data :** Due to cost, reading tables from BigQuery is done once, and the table is persisted in a folder, the function `read_public_data` can be used to read any public table present in BigQuery, you should provide the following arguments :
- Spark : The SparkSession.
- Project : The project name on GCP.
- Database : The database name containing the public data.
- Table : The table name on BigQuery, it can be replaced by an expression.
- Columns (optional): The list of the selected columns ( by default select all ), you can add as well bigquery functions.
- Condition (optional) : The condition added to the query ( by default no condition is provided ).

**Display :** you can specify some filters on the `main.py` file as follows : 
- max_date : The maximum date of the commits.
- min_date : The maximum date of the commits.
- format : The format of the date (ex: "YYYY-MM-DD" or  "YYYY-MM" ).
- language_list : A list of the languages that you want to display.


## Display the result

The analysis result is saved to an image format under the directory `result`

**Exemple :** 

![test](result/bigquery_github_analysis.png)