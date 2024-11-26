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