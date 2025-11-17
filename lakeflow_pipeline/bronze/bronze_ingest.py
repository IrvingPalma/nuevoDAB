# bronze/bronze_ingest.py
"""
Ingesta metadata-driven para todas las tablas Bronze.
Usa Auto Loader para procesar archivos incrementalmente.
"""
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from bronze_utils import add_audit_columns
from common.bronze_schemas import create_schema

@dp.table(
    name="bronze.clientes_raw",
    comment="Clientes raw desde CSV con Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false"
    }
)
def bronze_clientes_raw():
    """
    Ingesta incremental de clientes.
    Auto Loader detecta nuevos archivos automáticamente.
    """
    return add_audit_columns(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("delimiter", ",")
            .schema(create_schema())
            .load("/Volumes/retail_dev/default/myfiles/clientes*.csv")
    )

@dp.table(
    name="bronze.tiendas_raw",
    comment="Tiendas raw desde CSV",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"}
)
def bronze_tiendas_raw():
    return add_audit_columns(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load("/Volumes/retail_dev/default/myfiles/tiendas*.csv")
    )

@dp.table(
    name="bronze.productos_raw",
    comment="Productos raw desde CSV",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"}
)
def bronze_productos_raw():
    return add_audit_columns(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load("/Volumes/retail_dev/default/myfiles/productos*.csv")
    )

@dp.table(
    name="bronze.ventas_raw",
    comment="Ventas raw desde CSV - acepta múltiples patrones",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"}
)
def bronze_ventas_raw():
    """
    Ingesta de ventas con múltiples patrones posibles:
    - ventas*.csv
    - fact_sales*.csv
    - sales*.csv
    """
    return add_audit_columns(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load("/Volumes/retail_dev/default/myfiles/{ventas,fact_sales,sales}*.csv")
    )

@dp.table(
    name="bronze.devoluciones_raw",
    comment="Devoluciones raw desde CSV",
    table_properties={"quality": "bronze", "pipelines.reset.allowed": "false"}
)
def bronze_devoluciones_raw():
    return add_audit_columns(
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load("/Volumes/retail_dev/default/myfiles/devoluciones*.csv")
    )