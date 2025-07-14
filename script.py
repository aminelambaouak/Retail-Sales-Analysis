from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col
from pyspark.sql.types import DoubleType
import seaborn as sns
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RetailSalesAnalysis") \
    .getOrCreate()

# Define schema string for CSV
_schema = "ORDERNUMBER integer, QUANTITYORDERED integer, PRICEEACH decimal(10, 2), ORDERLINENUMBER integer, SALES decimal(10, 2), ORDERDATE timestamp, STATUS string, QTR_ID integer, MONTH_ID integer, YEAR_ID integer, PRODUCTLINE string, MSRP integer, PRODUCTCODE string, CUSTOMERNAME string, PHONE string, ADDRESSLINE1 string, ADDRESSLINE2 string, CITY string, STATE string, POSTALCODE string, COUNTRY string, TERRITORY string, CONTACTLASTNAME string, CONTACTFIRSTNAME string, DEALSIZE string"

# Read CSV with schema
new_df = spark.read.format('csv').schema(_schema).option("header", True).load("/home/amine/Downloads/sales_data_sample.csv")

# Drop unnecessary columns
new_df_1 = new_df.drop("ADDRESSLINE2", "ORDERDATE", "STATE")

# Capitalize column names
new_df_1 = new_df_1.toDF(*[col.capitalize() for col in new_df_1.columns])

# Rename some columns for clarity
new_columns = {
    "Quantityordered": "Quantity_ordered",
    "Priceeach": "Price_each",
    "Ordernumber": "Order_number",
    "Orderlinenumber": "Order_line_number",
    "Qtr_id": "Quarter",
    "Month_id": "Month",
    "Year_id": "Year",
    "Productline": "Product_line",
    "Productcode": "Product_code",
    "Customername": "Customer_name",
    "Addressline1": "Addressline",
    "Postalcode": "Postal_code",
    "Contactlastname": "Contact_last_name",
    "Contactfirstname": "Contact_first_name"
}

for old, new in new_columns.items():
    new_df_1 = new_df_1.withColumnRenamed(old, new)

# --------- Plot 1: Total Sales by Country ---------

country_df = new_df_1.groupBy("Country") \
    .agg(sum("Sales").alias("country_total_sales"))

country_df = country_df.withColumn("country_total_sales", col("country_total_sales").cast(DoubleType()))

country_pd = country_df.toPandas()
country_pd = country_pd.sort_values(by="Country", ascending=True)

plt.figure(figsize=(12, 6))
sns.barplot(data=country_pd, x="Country", y="country_total_sales")
plt.xticks(rotation=45)
plt.title("Total Sales by Country")
plt.xlabel("Country")
plt.ylabel("Country Total Sales")
plt.tight_layout()
plt.savefig("plot_country_total_sales.png")
plt.clf()

# --------- Plot 2: Top 10 Cities by Total Sales ---------

city_df = new_df_1.groupBy("City") \
    .agg(sum("Sales").alias("city_total_sales"))

city_df = city_df.withColumn("city_total_sales", col("city_total_sales").cast(DoubleType()))

city_pd = city_df.toPandas()
city_pd = city_pd.sort_values(by="city_total_sales", ascending=False).head(10)

plt.figure(figsize=(12, 6))
sns.barplot(data=city_pd, x="City", y="city_total_sales")
plt.xticks(rotation=45)
plt.title("Top 10 Cities by Total Sales")
plt.xlabel("City")
plt.ylabel("City Total Sales")
plt.tight_layout()
plt.savefig("plot_top10_cities_total_sales.png")
plt.clf()

# Stop Spark session if no longer needed
spark.stop()
