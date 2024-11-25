Step 1: Set Up GitHub Repository and Azure Resources
Create a GitHub Repository

Go to GitHub and create a new repository named casestudy.
While creating, select the option to initialize the repository with a README file.
Create Folder and YAML File in GitHub

Inside the casestudy repository, create a folder called retail.
Inside the retail folder, create a file named data.yml. This file will define the configurations for the data pipeline.
Create a Managed Identity (Micro-Entry)

Create a Managed Identity and name it microcase.
Assign the necessary roles and permissions to this managed identity as required for your resources.
Create Azure Key Vault

Create an Azure Key Vault named casekeyvault23.
Inside the Key Vault, create a Secret with the following details:
Secret Name: secretkey
Secret Value: R@moju23
Configure Access Policy for Key Vault

Go to the Access Policies section of the Key Vault.
Select all necessary permissions (like Get, List, Set) for secrets.
Add a contributor to the access policy.
Step 2: Gathering Data from HTTP API and SQL Database
Create Azure Storage Account

Create a Storage Account named casestrge23.
Create two containers inside the storage account:
raw (for storing raw data).
curated (for storing processed data).
Get Data from HTTP (Online API)

Visit https://fakestoreapi.com/products to get data from the online API.
Copy the URL for later use in the Data Factory pipeline.
Create SQL Database in Azure

Create a SQL Database Server with the following details:
Server Name: caseserver23
Database Name: casedatabase
User Login: sqlserveradmin
Password: R@moju23
Use the SalesLT.Product table to get data for the second source.
Step 3: Set Up Data Factory and GitHub Integration
Create Data Factory Account

Create an Azure Data Factory account named casedfactory.
Grant Access to Data Factory

Go to the Storage Account and navigate to IAM (Identity and Access Management).
Add the Data Factory ID with the necessary roles to grant access.
Configure GitHub Integration in Data Factory

Launch Azure Data Factory Studio.
Click on the Setup Repository option at the top left corner.
Choose GitHub as the version control system and provide the following GitHub details:
Repository Owner: Ramojuteju
Repository Name: casestudy
Branch: main
Click Create to complete the GitHub integration.
Step 4: Create Data Pipeline in Data Factory
Create New Pipeline

In Azure Data Factory, go to Author (left sidebar).
Click on the + symbol and select Pipeline.
In the Move & Transform section, select Copy Data.
Configure HTTP Source

In the Source section, click New and select HTTP.
Name the source as http and create a Linked Service called ls_http.
Set the Authentication Type to Anonymous.
Create a new linked service for Azure Key Vault (name it ls-azurekeyvault).
Test the connection and select secretkey from the dropdown.
Configure Sink (Destination)

In the Sink section, select Azure Data Lake Gen2.
Name the linked service as ls_httpsink and set the Format to JSON.
Set the Path to raw (the container you created earlier).
Create Trigger

Create a Trigger for this pipeline to run every 1 minute.
Save, Publish, Validate, and Debug

Save all pipeline activities.
Publish the pipeline.
Validate and debug the pipeline to ensure everything is configured correctly.
Verify Data in Raw Container

Go to your Raw container in Azure Storage.
Verify that the HTTP data has been saved as JSON files.
Step 5: Copy SQL Data to Data Lake
Create New Copy Data Activity

Go back to Author and create a new Copy Data activity.
Configure SQL Database Source

In the Source section, click New and choose SQL Database.
Name the source as sqldb and create a Linked Service called ls_sqldb.
Provide the SQL Server Name (caseserver23), Database Name (casedatabase), and other connection details.
Select the SalesLT.Product table.
Configure Sink (Destination)

In the Sink section, create a new Azure Data Lake Gen2 linked service:
Name the linked service ls_sqldbsink.
Set the Format to CSV.
Set the Path to raw (same container as for HTTP data).
Create Trigger

Create a new Trigger for this pipeline to run every 1 minute.
Save, Publish, Validate, and Debug

Save all pipeline activities.
Publish the pipeline.
Validate and debug the pipeline.
Verify SQL Data in Raw Container

Go to your Raw container in Azure Storage.
Verify that the SQL data has been saved as CSV files.
Step 6: Copy Data to Curated Container
Create New Copy Data Activity

Go back to Author and create a new Copy Data activity.
Configure Source and Sink

For the Source, select Azure Data Lake Gen2 and set the Path to the raw container (specifically the JSON file).
For the Sink, select Azure Data Lake Gen2 and set the Path to the curated container.
Save, Publish, Validate, and Debug

Save, publish, validate, and debug the pipeline.
Repeat for CSV Data

Repeat the same process for the CSV file. Set the Source to the raw container and select the CSV file.
Set the Sink to the curated container.
Verify Data in Curated Container

Go to the Curated container in Azure Storage.
Verify that both the JSON and CSV files are present.
Step 7: Connect GitHub to Data Factory
Since you have connected Data Factory to GitHub, all changes made in Azure Data Factory will be reflected in your GitHub repository.
Step 8: Set Up Databricks and Perform Data Operations
Create a Databricks Account

Create a Databricks workspace and log in.
Create a Databricks Cluster

Create a cluster named Ramoju.
Create a Databricks Folder and Notebook

In Databricks, create a folder named casestudy.
Create a notebook inside the casestudy folder and name it casestudynote.
Read and Write Data in Delta Format

In your Databricks notebook, use the following PySpark code to read from the curated folder and write data to the meta folder in Delta format:
python
Copy code
# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.casestrge23.dfs.core.windows.net",
    "yeK+Hcii16ltwTQ0JHLYSh5/37LXjgW+CBr1+L41m1XlvHBZLeA3Rp0JjwGggfePoEzFGGDyyKWY+ASt9KkQqA==")

# COMMAND ----------

# Displaying Data Frame in Tabular Format

display(dbutils.fs.ls("abfss://curated@casestrge23.dfs.core.windows.net"))

# COMMAND ----------

# Reading data from curated container

http_df = spark.read.csv("abfss://curated@casestrge23.dfs.core.windows.net/1b55c3a7-2b47-4b7d-8df7-0a629b942853", header=True, inferSchema="true");
http_df.show()
sqldb_df = spark.read.csv("abfss://curated@casestrge23.dfs.core.windows.net/SalesLT.Product.txt", header=True, inferSchema="true");
sqldb_df.show()

# COMMAND ----------

# Define the paths to the meta container
meta_http = "abfss://meta@casestrge23.dfs.core.windows.net/delta/http_delta"
meta_sqldb = "abfss://meta@casestrge23.dfs.core.windows.net/delta/sqldb_delta"

# COMMAND ----------

import re

# Function to clean column names
def clean_column_names(df):
    # Replace invalid characters with underscores or any other valid character
    new_columns = [re.sub(r'[ ,;{}()\n\t=]', '_', col) for col in df.columns]
    df = df.toDF(*new_columns)
    return df

# Apply the function to your DataFrame
http_df_cleaned = clean_column_names(http_df)
sqldb_df_cleaned = clean_column_names(sqldb_df)

# Now write the cleaned DataFrames
http_df_cleaned.write.format("delta").mode("append").save(meta_http)
sqldb_df_cleaned.write.format("delta").mode("append").save(meta_sqldb)


# COMMAND ----------

# Enable column mapping for the Delta table
spark.sql(f"ALTER TABLE delta.`{meta_http}` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
spark.sql(f"ALTER TABLE delta.`{meta_sqldb}` SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")

# Save the cleaned DataFrames to the meta container with schema evolution enabled
http_df.write.format("delta").mode("append").option("mergeSchema", "true").save(meta_http)
sqldb_df.write.format("delta").mode("append").option("mergeSchema", "true").save(meta_sqldb)

# COMMAND ----------

# Displaying Data Frame in Tabular Format
display(dbutils.fs.ls("abfss://meta@casestrge23.dfs.core.windows.net"))

Run the notebook to perform the necessary actions of reading and writing the data.
