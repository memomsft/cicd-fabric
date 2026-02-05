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

# MARKDOWN ********************


# CELL ********************

from pyspark.sql.functions import col

# Lee la tabla del mirroring
df_source = spark.table("fact_reservaciones_snowflake")

# Casting específico - NOTA: usa backticks `` para METADATA$ROW_ID
df_typed = df_source.select(
    # IDs - De Double/Complex a Long
    col("RESERVACION_ID").cast("long"),
    col("HUESPED_ID").cast("long"),
    col("PROPIEDAD_ID").cast("long"),
    
    # Fechas
    col("FECHA_CHECKIN").cast("date"),
    col("FECHA_CHECKOUT").cast("date"),
    
    # Números enteros
    col("NOCHES").cast("int"),
    
    # Strings
    col("HABITACION_TIPO").cast("string"),
    col("CANAL_RESERVACION").cast("string"),
    col("ESTADO_RESERVACION").cast("string"),
    
    # Decimal
    col("TARIFA_PROMEDIO_NOCHE").cast("decimal(10,2)"),
    
    # Timestamp
    col("TIMESTAMP_ACTUALIZACION").cast("timestamp"),
    
    # Metadata - USA BACKTICKS para el $
    col("`METADATA$ROW_ID`").cast("string").alias("METADATA_ROW_ID")
)

# Guarda la tabla clonada con tipos correctos
df_typed.write \
    .mode("overwrite") \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable("fact_reservaciones_typed")

print("✅ Tabla clonada con tipos correctos")

# Verifica el schema
print("\n📊 Schema de la tabla nueva:")
spark.table("fact_reservaciones_typed").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
