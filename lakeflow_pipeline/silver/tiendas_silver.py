# silver/tiendas_silver.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.table(
    name="bronze.tiendas_clean",
    comment="Tiendas limpias listas para CDC",
    table_properties={"quality": "bronze"}
)
@dp.expect_or_drop("valid_id_tienda", "id_tienda IS NOT NULL")
def bronze_tiendas_clean():
    """Limpieza de tiendas"""
    df = spark.readStream.table("bronze.tiendas_raw")
    
    return df.select(
        F.col("id_tienda").cast(LongType()),
        F.initcap(F.trim(F.col("nombre"))).alias("nombre"),
        F.initcap(F.trim(F.col("ciudad"))).alias("ciudad"),
        F.upper(F.trim(F.col("region"))).alias("region"),
        F.col("_created_at")  # ← Mantener este campo de Bronze
    )

dp.create_streaming_table(
    name="silver.tiendas",
    comment="Estado actual de tiendas",
    schema="""
        id_tienda BIGINT NOT NULL,
        nombre STRING,
        ciudad STRING,
        region STRING,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

dp.create_auto_cdc_flow(
    target="silver.tiendas",
    source="bronze.tiendas_clean",
    keys=["id_tienda"],
    sequence_by="_created_at",  # ← Ahora sí existe porque lo incluimos en el select
    stored_as_scd_type=1,
    name="tiendas_cdc_flow"
)