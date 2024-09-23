# Microsoft Fabric Project
This project implements a centralized Data Engineering Structure on Microsoft Fabric. 
It involves ingesting data into a Fabric-based Lakehouse and processing it through the Medallion Architecture, which comprises Bronze, Silver, and Gold layers.

# Table of Contents
Architecture Diagram
![image](https://github.com/user-attachments/assets/752f7ed5-286f-4563-95d3-e579d329e19a)

# Data Source
## 1st data source - Files on Storage Account ADSL Gen2 on Azure. 
To create the Storage you have to go to the Azure Portal and create inside a previosly created resource group a Storage Account. 

On this project we selected a Block Blob, that is recommended for scenarios with high transaction rates or that use smaller objects or require consistently low storage latency, according to Microsoft.

Another option that you need to activate is the hierachical namespace, where you enable file-level access control lists (ACLs), also enabling Azure Data Lake Storage Gen2 that is necessary to create a shortcut in our lakehouse.

## 2nd data source - .csv documents on OneLake storage
The OneLake storage works like a Onedrive storage, where you just drag and drop files in a folder on you computer, uploading to the Onelake. 
Here we can transform the file in Delta Table manually or create automations throught Dataflows Gen2 or Spark Notebook to populate the tables.
