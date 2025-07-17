# Netflix-Azure-Data-Engineering-Project

This Project contains end to end data engineering project for netflix data. The first step is to load all csv files (except master file) under bronze layer into azure data lake using azure data factory and parameterised pipeline.
The next step is to load the master file in bronze layer incrementally using azure databricks autoloader.
Then prepared silver layer after transforming raw data in databricks pyspark and upload delta format of all files under silver layer through parameterised notebook and databricks workflow.
Then create delta live table (DLT framework) on top of delta files residing in silver layer. Also added some exceptions in DLT pipeline for managing data quality checks. So the flow is creating streaming table on all delta files except master delta file then create streaming table for staging master data, streaming view for transformed master data and finally streaming table for aggregated master data.
The last step is to create a databricks connection to PowerBI for further reports and dashboard preparation. 
