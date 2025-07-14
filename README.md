Retail Sales Analysis with PySpark
Description

This project analyzes retail sales data using PySpark. It loads a CSV dataset, cleans and transforms it, and produces visualizations showing:

    Total sales by country

    Top 10 cities by total sales

The results are saved as PNG images.
Features

    Load data with a defined schema in Spark

    Clean and rename columns for clarity

    Aggregate sales data by country and city

    Convert Spark DataFrames to Pandas for plotting

    Create bar plots using Seaborn and Matplotlib

    Save plots as images

Requirements

    Python 3.x

    PySpark

    pandas

    seaborn

    matplotlib

Install dependencies with:

pip install pyspark pandas seaborn matplotlib

Usage

    Place your CSV file (e.g., sales_data_sample.csv) in the specified path.

    Run the Python script.

    The plots will be saved as plot_country_total_sales.png and plot_top10_cities_total_sales.png.

Notes

    Adjust the CSV path in the script if needed.

    The Spark session is stopped at the end to free resources.