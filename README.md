# Microsoft Fabric Project
This project implements a centralized Data Engineering Structure on Microsoft Fabric. 
It involves ingesting data into a Fabric-based Lakehouse and processing it through the Medallion Architecture, which comprises Bronze, Silver, and Gold layers. 

# Architecture
Architecture Diagram
![image](https://github.com/user-attachments/assets/752f7ed5-286f-4563-95d3-e579d329e19a)

# Azure Data Factory - Data Source 1
## Overview
This part demonstrates data ingestion from Oracle into Azure using Azure Data Factory (ADF), following the best practices of the Medallion Architecture. To store raw data, I opted for .parquet files in the Bronze (Raw) layer, as recommended by Microsoft’s Medallion Lakehouse Architecture.

https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture#deployment-model

By using parquet format, I achieved significant compression—up to 89%. For example, one day of transaction data, originally 1.235 GB, was compressed down to 118 MB, providing both storage efficiency and optimized data processing (Figure 1).

![image](https://github.com/user-attachments/assets/a1a8323b-684c-4af1-985b-7e2add6c9da0) (Figure 1)

## Data Ingestion Process
The data ingestion pipeline in Azure Data Factory, depicted in Figure 3, incorporates **LookUp** and **ForEach** activities. I opted to create one parquet file for each day’s data. The ForEach activity loops over the data, and within it, a Copy Data activity processes the items retrieved by the LookUp activity.

![image](https://github.com/user-attachments/assets/1afb046f-074c-4ac8-bac1-4048b8f9b303)(Figure 2)

## Incremental Load Logic
For each **LookUp** activity, I perform a query on the table based on incremental logic defined by business rules. Some tables are reprocessed for data ranging from 4 days to one month prior to the current date. Others are entirely overwritten because they are smaller (under 1 million rows).

Each parquet file in the Bronze layer is named using the corresponding data date. If a file with the same name already exists, it is replaced to prevent duplication. The folder structure is organized by the date of the fact data.

The .parquet files and folder path appended in the bronze layer is named using the date of that data, using the following expression in the linked service:

```json
"folderPath": { "value": "raw/fact_table/incremental/@{formatDateTime(item().date_column, 'yyyy')}"
```

The folders will look like this:
![image](https://github.com/user-attachments/assets/3488c83d-9e43-4826-bb50-90743921fa92)

```json
fileName": { "value": "fact_table_@{item().date}", "type": "Expression"  }
```

The files, will look like this:

![image](https://github.com/user-attachments/assets/0d95d44a-278a-41ce-977a-44c5d28ba55b)

The fact tables needed to be reprocessed regularly. In the Bronze (Raw) layer, all files are replaced when a new file with the same name is encountered. This approach prevents file duplication and ensures that only the latest data is retained. The storage structure is organized by date, making it easy to manage the data over time.

For dimension tables, which don’t require reprocessing, I implemented a different logic. Currently, I am ingesting almost 50 tables daily from multiple Oracle databases. To avoid creating a separate pipeline for each table and to keep the Azure Data Factory more organized, I designed the following logic, as demonstrated in the figure below.

![image](https://github.com/user-attachments/assets/31f90e97-11de-4716-bf06-11f3c1158fc6)

I utilize a lookup process based on an aux_ingest.csv file, which contains two key columns. The first column specifies the schema where the table is located (either Schema A or Schema B), and the second column lists the exact table name as it appears in the database.

Additionally, there are two filters in place to allow for the selection of specific tables for reprocessing, if necessary. This process is further illustrated in Table 1.

![image](https://github.com/user-attachments/assets/b2a7bc22-d179-428c-b358-53948c5ea08b)

Filters A and B are used to segment data by database, while the ForEach activity is configured to handle each database individually. To manage file organization in OneLake, I implemented an expression that dynamically creates a folder named after the table being processed:

```json
"folderPath": {
    "value": "raw/@{item().table}",
    "type": "Expression"
}
```

The files in bronze layer will looks like this:
![image](https://github.com/user-attachments/assets/1f76e35c-cafe-4b90-90d2-3bd08fd4cf95)

![image](https://github.com/user-attachments/assets/7fcf0050-b35e-4550-b1e6-25e2bf7f66a4)



# Processing data on Microsoft Fabric
With the Parquet files stored in OneLake, the Bronze layer is ready for processing to create Delta Tables in the Silver layer, which is the default format in Microsoft Fabric.

It's worth noting that while code suggestions are always welcome, there are times when we don’t have the luxury of refining everything for optimal performance—**you just need to deploy and move forward**. That being said, I chose to use Spark Notebooks for processing because, based on previous experience, POCs, and the following material, this approach appears to be both cost-effective and faster on Microsoft Fabric:

[Comparing Dataflow Gen2 vs Notebook on Costs and Usability](https://www.fourmoo.com/2024/01/25/microsoft-fabric-comparing-dataflow-gen2-vs-notebook-on-costs-and-usability/)

## The first ingestion 
For the initial ingestion, we need to populate all the data from the Bronze layer into Delta Tables in the Silver layer. At this stage, my focus will be on two primary aspects: data types and partitions.

In classic Medallion Architecture, the recommendation is to prepare data for self-service BI and delivery using a semantic model, which would be ideal, especially in Microsoft Fabric. However, in my case, the business requires access to all data, which has its benefits from a Data Engineering perspective. This helps determine which tables need preparation for the Gold layer.

Below is the code used for the ingestion process. As mentioned, the first step is to address data types by defining the schema, as demonstrated in the schema code below.

```python
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType, DateType

schema = StructType([
    StructField("column1", DateType(), True),
    StructField("column2", IntegerType(), True),
    StructField("column3", IntegerType(), True),
    StructField("column4", IntegerType(), True)
])
```

## Optimization with Partitioning
After addressing data types, we can proceed with optimizing the data. The .partitionBy function in Spark is highly beneficial for tables with large volumes of data. When Delta Tables are created, multiple .parquet files are generated in the underlying storage. Without partitioning, Spark needs to scan all these files—essentially behaving as if it were reading every book in a library to find what you're looking for. By applying partitions, you can label each "book" (file) by specific attributes, such as year, month, day, or hour, reducing the scan time significantly.

In my case, each day's data ranges between 80MB and 110MB. After running several POCs, I decided to partition the data by year and month to strike a balance between file size and performance. Once a month is complete, we can run the OPTIMIZE function (to be discussed later) to further consolidate and optimize these files.

Below is the code used for writing and partitioning the files.

```python
from pyspark.sql.functions import year, month, dayofmonth, col, date_format

# Define the source path
file_source = "abfss://xxxxxxxxxxx@onelake.dfs.fabric.microsoft.com/xxxxxxxxx/Files/raw/fact_table/"

# Read the data from the source
df = spark.read.schema(schema).parquet(file_source)

# Define the schema for the columns and cast data types accordingly
for field in schema.fields:
    if field.name in df.columns:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    else:
        print(f"Column not found: {field.name}")

# Add day, month, and year columns for delta table optimization
df_output = df.withColumn("day", dayofmonth(col("date")).cast("integer")) \
              .withColumn("month", month(col("date")).cast("integer")) \
              .withColumn("year", year(col("date")).cast("integer"))

# Write to the delta table, partitioned by year and month
df_output.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("year", "month") \
    .saveAsTable("Silver_Layer.fact_table")

```

## Reprocessing Fact Tables
Certain business rules require reprocessing specific dates within the fact tables, depending on the needs of each sector.

Reprocessing D-4
The first step in this process involves using a Spark notebook to delete existing data from the fact table before ingesting the new data. The logic applied is to delete all records starting from two days before the current maximum date. After that, new data is ingested starting from this adjusted maximum date onward.

Below is the code used for deleting data:

```python
from pyspark.sql import SparkSession
from datetime import timedelta

# Initialize the Spark session
spark = SparkSession.builder.appName("DeleteData").getOrCreate()

# Get the max date from the fact table
max_date_query = spark.sql("SELECT MAX(date) AS max_date FROM schema.fact_table")
max_date_row = max_date_query.collect()[0]
max_date = max_date_row['max_date']

# Calculate the limit date (D-3)
limit_date = max_date - timedelta(days=2)

# Format the limit date as a string, if necessary
limit_date_str = limit_date.strftime('%Y-%m-%d')

# Execute the DELETE query with the calculated limit date
delete_query = f"DELETE FROM schema.fact_table WHERE date >= '{limit_date_str}'"
spark.sql(delete_query)

# Optionally, display the limit date for verification
print(f"Data deleted starting from: {limit_date_str}")
```

To ensure that after deleting the data you are re-ingesting with the correct data types, you should set the schema properly before writing the new data back to the Delta table.

```python
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType, DateType

schema = StructType([
    StructField("column1", DateType(), True),
    StructField("column2", IntegerType(), True),
    StructField("column3", IntegerType(), True),
    StructField("column4", IntegerType(), True)
])
```

With the schema defined, now we insert the data that was ingested in the date that was reprocessed.

```python
from pyspark.sql.functions import year, month, dayofmonth, col, date_format

# Define the source path for incremental data
file_source_incremental = "path_to_incremental_files"

# Get the current max date from the Delta table
max_date_populate = spark.sql("SELECT MAX(date) AS max_date FROM schema.fact_table")
max_date_row = max_date_populate.collect()[0]
max_date = max_date_row['max_date']

# Read the incremental data from the source
df = spark.read.option("schema", schema).parquet(file_source_incremental)

# Filter the data where the date is greater than the current max date
df = df.where(col("date") > max_date)

# Cast all columns to the correct data types based on the schema
for field in schema.fields:
    if field.name in df.columns:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    else:
        print(f"Column not found: {field.name}")

# Add columns for day, month, and year for optimization and partitioning
df_output = df.withColumn("DIA", dayofmonth(col("date")).cast("integer")) \
              .withColumn("MES", month(col("date")).cast("integer")) \
              .withColumn("ANO", year(col("date")).cast("integer"))

# Write the data to the Delta table in append mode for incremental loads
df_output.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("ANO", "MES") \
    .saveAsTable("schema.fact_table")

# Display the output dataframe
display(df_output)
```
