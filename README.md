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


