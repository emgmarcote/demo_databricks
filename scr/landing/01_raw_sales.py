# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, avg, count, sum as sum_

# COMMAND ----------
# BRONZE LAYER - Datos raw

datos = [
    (1, "2024-01-15", "Laptop", "Electrónica", 5, 899.99),
    (2, "2024-01-16", "Mouse", "Accesorios", 10, 29.99),
    (3, "2024-01-16", "Teclado", "Accesorios", 7, 79.99),
    (4, "2024-01-17", "Monitor", "Electrónica", 3, 299.99),
    (5, "2024-01-17", "Webcam", "Accesorios", 8, 49.99)
]

columnas = ["venta_id", "fecha", "producto", "categoria", "cantidad", "precio_unitario"]

df_bronze = spark.createDataFrame(datos, columnas)

# Guardar en formato Delta
df_bronze.write.format("delta").mode("overwrite").saveAsTable("ventas_bronze")

print("✓ Capa Bronze creada")
display(df_bronze)

# COMMAND ----------
# SILVER LAYER - Datos limpios y validados

df_silver = (
    spark.table("ventas_bronze")
    .filter(col("cantidad") > 0)
    .filter(col("precio_unitario") > 0)
    .withColumn("total_venta", col("cantidad") * col("precio_unitario"))
    .withColumn("timestamp_proceso", current_timestamp())
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("ventas_silver")

print("✓ Capa Silver creada")
display(df_silver)

# COMMAND ----------
# GOLD LAYER - Agregaciones para análisis

df_gold = (
    spark.table("ventas_silver")
    .groupBy("categoria")
    .agg(
        count("venta_id").alias("num_ventas"),
        sum_("cantidad").alias("cantidad_total"),
        sum_("total_venta").alias("ingresos_totales"),
        avg("precio_unitario").alias("precio_promedio")
    )
    .orderBy(col("ingresos_totales").desc())
)

df_gold.write.format("delta").mode("overwrite").saveAsTable("ventas_gold")

print("✓ Capa Gold creada")
display(df_gold)

# COMMAND ----------
# Verificar las tablas creadas

print("Tablas disponibles:")
spark.sql("SHOW TABLES").show()

# Consulta SQL de ejemplo
spark.sql("""
    SELECT 
        categoria,
        num_ventas,
        ingresos_totales,
        ROUND(precio_promedio, 2) as precio_promedio
    FROM ventas_gold
""").show()