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

"folderPath": { "value": "raw/fact_table/incremental/@{formatDateTime(item().date_column, 'yyyy')}"

fileName": { "value": "fact_table_@{item().date}", "type": "Expression"  }




