🛒 Retail Sales Analysis with PySpark

This project performs a data analysis on a retail sales dataset using PySpark, with the goal of understanding sales distribution across countries and cities.
Objective

    Analyze retail sales data by country and city.

    Identify the top-performing countries and cities based on total sales.

    Generate visualizations for business insights.

Technologies Used

    PySpark for big data processing

    Pandas for intermediate data handling

    Matplotlib and Seaborn for data visualization

    Jupyter Notebook or Python script (.py) for execution

Dataset

Format: CSV
Sample Columns: ORDERNUMBER, QUANTITYORDERED, PRICEEACH, SALES, CITY, COUNTRY, CUSTOMERNAME, PRODUCTLINE

Dataset path used in the code:
"/home/amine/Downloads/sales_data_sample.csv"
Main Steps

    Data Loading:
    Load the dataset using a custom schema via spark.read.csv.

    Data Cleaning:
    Drop unused columns.
    Capitalize and rename column names for better readability.

    Aggregation:
    Total sales by country.
    Top 10 cities by total sales.

    Visualization:
    Bar charts with Seaborn and Matplotlib.

Output Visualizations

    Total Sales by Country

    Top 10 Cities by Total Sales

Key Insights

    The USA leads in total country sales.

    Madrid, San Rafael, and NYC are the top three cities in total sales.

    These insights can guide marketing and logistics decisions.

Future Improvements

    Use Spark SQL for more complex querying

    Deploy the pipeline using Databricks or Apache Airflow

    Save results in Parquet/Delta format for optimized storage

    Build a dashboard using Power BI or Tableau

    Add time-series analysis for trend detection

Author

Amine Lambaouak

📧 aminelambaouak@gmail.com

🔗 https://www.linkedin.com/in/amine-lambaouak-656575172/
