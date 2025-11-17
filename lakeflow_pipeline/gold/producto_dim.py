from pyspark.sql import functions as F
from pyspark import pipelines as dp


# ================================================================
# DIMENSIÓN PRODUCTO (sin surrogate key)
# ================================================================

@dp.table(
    name="silver.productos_enriched",
    comment="Productos preparados para Gold"
)
def silver_productos_enriched():
    return (
        spark.readStream.table("silver.productos")
        .select(
            "id_producto",
            "nombre",
            "categoria",
            "precio",
            "_created_at"
        )
        .withColumn("creation_date", F.current_timestamp())
    )
# Dimensión Producto
dp.create_streaming_table(
    name="gold.dim_producto",
    comment="Dimensión Producto - Clave natural: id_producto",
    schema="""
        id_producto BIGINT NOT NULL,
        nombre STRING,
        categoria STRING,
        precio DECIMAL(18,2),
        creation_date TIMESTAMP,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

dp.create_auto_cdc_flow(
    target="gold.dim_producto",
    source="silver.productos_enriched",
    keys=["id_producto"],
    sequence_by="_created_at",
    stored_as_scd_type=1,
    name="dim_producto_cdc_flow"
)
