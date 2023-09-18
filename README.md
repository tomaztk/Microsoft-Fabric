# Microsoft Fabric

## Data Engineering

### Create delta tables

Preparation

```
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

from pyspark.sql.functions import col, year, month, quarter

table_name = 'fact_sale'

df = spark.read.format("parquet").load('Files/wwi-raw-data/full/fact_sale_1y_full')
df = df.withColumn('Year', year(col("InvoiceDateKey")))
df = df.withColumn('Quarter', quarter(col("InvoiceDateKey")))
df = df.withColumn('Month', month(col("InvoiceDateKey")))

df.write.mode("overwrite").format("delta").partitionBy("Year","Quarter").save("Tables/" + table_name)

from pyspark.sql.types import *

def loadFullDataFromSource(table_name):
    df = spark.read.format("parquet").load('Files/wwi-raw-data/full/' + table_name)
    df.write.mode("overwrite").format("delta").save("Tables/" + table_name)

full_tables = [
    'dimension_city',
    'dimension_date',
    'dimension_employee',
    'dimension_stock_item'
    ]

for table in full_tables:
    loadFullDataFromSource(table)
    
```

Data Transformation

```
df_fact_sale = spark.read.table("wwilakehouse.fact_sale") 
df_dimension_date = spark.read.table("wwilakehouse.dimension_date")
df_dimension_city = spark.read.table("wwilakehouse.dimension_city")

sale_by_date_city = df_fact_sale.alias("sale") \
.join(df_dimension_date.alias("date"), df_fact_sale.InvoiceDateKey == df_dimension_date.Date, "inner") \
.join(df_dimension_city.alias("city"), df_fact_sale.CityKey == df_dimension_city.CityKey, "inner") \
.select("date.Date", "date.CalendarMonthLabel", "date.Day", "date.ShortMonth", "date.CalendarYear", "city.City", "city.StateProvince", "city.SalesTerritory", "sale.TotalExcludingTax", "sale.TaxAmount", "sale.TotalIncludingTax", "sale.Profit")\
.groupBy("date.Date", "date.CalendarMonthLabel", "date.Day", "date.ShortMonth", "date.CalendarYear", "city.City", "city.StateProvince", "city.SalesTerritory")\
.sum("sale.TotalExcludingTax", "sale.TaxAmount", "sale.TotalIncludingTax", "sale.Profit")\
.withColumnRenamed("sum(TotalExcludingTax)", "SumOfTotalExcludingTax")\
.withColumnRenamed("sum(TaxAmount)", "SumOfTaxAmount")\
.withColumnRenamed("sum(TotalIncludingTax)", "SumOfTotalIncludingTax")\
.withColumnRenamed("sum(Profit)", "SumOfProfit")\
.orderBy("date.Date", "city.StateProvince", "city.City")

sale_by_date_city.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save("Tables/aggregate_sale_by_date_city")
     
```

### EDA

```
# Load data from source
df = spark.read.load("Tables/csv_folder", header=True, inferSchema=True)

df.count()

from pyspark.sql.functions import col

df.groupBy(col("vendorID")).count().show()


# Retrieve information about the earliest and latest pickup dates in the dataset.

from pyspark.sql.functions import min, max

oldest_day = df.select(min("lpep_pickup_datetime")).collect()[0][0]
latest_day = df.select(max("lpep_dropoff_datetime")).collect()[0][0]

print("Oldest pickup date: ", oldest_day)
print("Latest pickup date: ", latest_day)

```
Aggregation

```
from pyspark.sql.functions import col, year, month, dayofmonth, avg


average_fare_per_month = (
    df
    .groupBy(year("lpep_pickup_datetime").alias("year"), month("lpep_pickup_datetime").alias("month"))
    .agg(avg("fare_amount").alias("average_fare"))
    .orderBy("year", "month")
)
display(average_fare_per_month)

average_fare_per_month.write.format("delta").mode("overwrite").saveAsTable("average_fare_per_month")
```



## Data Science

```
import os
import gzip

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

import numpy as np
import pandas as pd

import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.style as style
import seaborn as sns

%matplotlib inline

from synapse.ml.featurize import Featurize
from synapse.ml.core.spark import FluentAPI
from synapse.ml.lightgbm import *
from synapse.ml.train import ComputeModelStatistics

import mlflow


transformer = Featurize().setOutputCol("features").setInputCols(FEATURE_COLUMNS).fit(raw_df)
df = transformer.transform(raw_df)

train_df.groupby(TREATMENT_COLUMN).count().show()


treatment_train_df = train_df.where(f"{TREATMENT_COLUMN} > 0")
control_train_df = train_df.where(f"{TREATMENT_COLUMN} = 0")


classifier = (
    LightGBMClassifier()
    .setFeaturesCol("features")  # Set the column name for features
    .setNumLeaves(10)  # Set the number of leaves in each decision tree
    .setNumIterations(100)  # Set the number of boosting iterations
    .setObjective("binary")  # Set the objective function for binary classification
    .setLabelCol(LABEL_COLUMN)  # Set the column name for the label
)

# Start a new MLflow run with the name "uplift"
active_run = mlflow.start_run(run_name="uplift")

# Start a new nested MLflow run with the name "treatment"
with mlflow.start_run(run_name="treatment", nested=True) as treatment_run:
    treatment_run_id = treatment_run.info.run_id  # Get the ID of the treatment run
    treatment_model = classifier.fit(treatment_train_df)  # Fit the classifier on the treatment training data

# Start a new nested MLflow run with the name "control"
with mlflow.start_run(run_name="control", nested=True) as control_run:
    control_run_id = control_run.info.run_id  # Get the ID of the control run
    control_model = classifier.fit(control_train_df)
    
loaded_treatmentmodel = mlflow.spark.load_model(treatment_model_uri, dfs_tmpdir="Files/spark")
loaded_controlmodel = mlflow.spark.load_model(control_model_uri, dfs_tmpdir="Files/spark")

# Make predictions
batch_predictions_treatment = loaded_treatmentmodel.transform(test_df)
batch_predictions_control = loaded_controlmodel.transform(test_df)
batch_predictions_treatment.show(5)
 
```

## Real time analytics

## CI/CD + Development



Under MIT license.