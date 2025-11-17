# silver/productos_silver.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.productos_clean",
    comment="Productos limpios listos para CDC",
    table_properties={"quality": "bronze"}
)
@dp.expect_or_drop("valid_id_producto", "id_producto IS NOT NULL")
@dp.expect_or_drop("valid_precio", "precio > 0")
def bronze_productos_clean():
    """Limpieza de productos"""
    df = spark.readStream.table("bronze.productos_raw")
    
    # Limpieza de nombre
    df = df.withColumn("nombre", F.initcap(F.trim(F.col("nombre"))))
    
    # Limpieza de categoría
    df = df.withColumn(
        "categoria",
        F.when(F.col("categoria").isNull(), None)
         .otherwise(F.initcap(F.trim(F.col("categoria"))))
    )
    
    # Limpieza de precio
    df = df.withColumn(
        "precio_num",
        F.expr("try_cast(precio as DOUBLE)")
    )
    
    df = df.withColumn(
        "precio",
        F.when(
            (F.col("precio_num").isNull()) | (F.col("precio_num") <= 0),
            None
        ).otherwise(F.round(F.col("precio_num"), 2))
    ).drop("precio_num")
    
    return df.select(
        F.col("id_producto").cast(LongType()),
        F.col("nombre").cast(StringType()),
        F.col("categoria").cast(StringType()),
        F.col("precio").cast(DecimalType(18, 2)),
        F.col("_created_at")  # ← Mantener este campo de Bronze
    )

dp.create_streaming_table(
    name="silver.productos",
    comment="Estado actual de productos",
    schema="""
        id_producto BIGINT NOT NULL,
        nombre STRING,
        categoria STRING,
        precio DECIMAL(18,2),
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

dp.create_auto_cdc_flow(
    target="silver.productos",
    source="bronze.productos_clean",
    keys=["id_producto"],
    sequence_by="_created_at",  # ← Ahora sí existe
    stored_as_scd_type=1,
    name="productos_cdc_flow"
)