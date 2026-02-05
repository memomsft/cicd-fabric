# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ffda55c2-ea1a-4b83-9779-8ddd90c0f347",
# META       "default_lakehouse_name": "LH_CONTOSO_ANALYTICS",
# META       "default_lakehouse_workspace_id": "164dde95-5d2c-4714-8021-2eab351acea6",
# META       "known_lakehouses": [
# META         {
# META           "id": "ffda55c2-ea1a-4b83-9779-8ddd90c0f347"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_CONTOSO_ANALYTICS.fact_reservaciones_snowflake")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE TABLE maestros_dim_huespedes_PostgreSQL;
# MAGIC TRUNCATE TABLE maestros_dim_propiedades_PostgreSQL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
