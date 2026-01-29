import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Databricks notebook source
import dlt

# Crear una view (no materializada)
@dlt.view(
    name="productos_raw",
    comment="View temporal con datos de productos"
)
def productos_raw():
    datos = [
        (1, "Laptop", "Electrónica", 899.99, 50),
        (2, "Mouse", "Accesorios", 29.99, 200),
        (3, "Teclado", "Accesorios", 79.99, 150),
        (4, "Monitor", "Electrónica", 299.99, 75),
        (5, "Webcam", "Accesorios", 49.99, 100)
    ]
    
    columnas = ["producto_id", "nombre", "categoria", "precio", "stock"]
    return spark.createDataFrame(datos, columnas)

# Tabla materializada desde la view
@dlt.table(
    name="productos",
    comment="Tabla de productos con validaciones"
)
@dlt.expect_or_fail("precio_valido", "precio > 0")
@dlt.expect_or_fail("stock_no_negativo", "stock >= 0")
def productos():
    return dlt.read("productos_raw")