#!/usr/bin/env python
# coding: utf-8

# In[8]:


from pyspark.sql import SparkSession

# Initialize Spark session 
spark = SparkSession.builder \
    .appName("OLAPCH") \
    .getOrCreate()


# In[9]:


# 1. Cragamos nuestro csv de ventas a un DataFrame  (RAW --> BRONZE)
df_raw = spark.read.csv("/data/ventas_diarias.csv", header=True)

# 2. Transformamos para ajustarnos al esquema de la Tabla de Hechos (Transformaciones --> SILVER)
fact_ventas = df_raw.select("id_venta", "fecha", "id_producto", "cantidad", "total")


# In[10]:


# Obtener los IDs únicos de producto como un DataFrame para verificar que los tipos correspondan si no hay que realizar transformaciones
productos_unicos_df = df_raw.select("id_producto").distinct()

# verlos en una lista de python
lista_ids = [row.id_producto for row in productos_unicos_df.collect()]  #collect no es una transformacion es una acción

print(f"Total de productos únicos: {len(lista_ids)}")
print(lista_ids[:10]) # Ver los primeros 10


# In[11]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

# Creamos la tabla de dimensión asignando un ID numérico correlativo
dim_productos = productos_unicos_df.withColumn(
    "id_prod_numeric", 
    row_number().over(Window.orderBy("id_producto"))
)

dim_productos.show(5)


# In[12]:


# Unimos el dataframe original con la dimensión para traer el ID numérico
df_para_clickhouse = df_raw.join(dim_productos, "id_producto", "left") \
    .select(
        col("id_venta").cast("long"),
        col("fecha").cast("date"),
        col("id_prod_numeric").alias("id_producto").cast("int"), # El ID ya es número!
        col("cantidad").cast("int"),
        col("total").alias("total_venta").cast("double")
    )


# In[13]:


from pyspark.sql import SparkSession

# Get the active SparkSession
active_spark_session = SparkSession.getActiveSession()

if active_spark_session:
    print("Active SparkSession:", active_spark_session)
else:
    print("No active SparkSession found.")


# In[14]:


import sys

try:

    # Ruta de salida (puede ser local o HDFS/S3)
    ruta_salida = "/data/parquets"

    # Escribir en formato Parquet
    df_para_clickhouse.write.mode("overwrite").parquet(ruta_salida)

    print(f" Archivo Parquet guardado en: {ruta_salida}")

except Exception as e:
    print(f" Error al escribir Parquet: {e}", file=sys.stderr)

finally:
    spark.stop()


# In[ ]:




