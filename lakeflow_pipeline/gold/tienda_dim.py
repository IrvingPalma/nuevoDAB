from pyspark.sql import functions as F
from pyspark import pipelines as dp


# ================================================================
# DIMENSIÓN TIENDA (sin surrogate key)
# ================================================================

@dp.table(
    name="silver.tiendas_enriched",
    comment="Tiendas preparadas para Gold"
)
def silver_tiendas_enriched():
    return (
        spark.readStream.table("silver.tiendas")
        .select(
            "id_tienda",
            "nombre",
            "ciudad",
            "region",
            "_created_at"
        )
        .withColumn("creation_date", F.current_timestamp())
    )


# Dimensión Tienda
dp.create_streaming_table(
    name="gold.dim_tienda",
    comment="Dimensión Tienda - Clave natural: id_tienda",
    schema="""
        id_tienda BIGINT NOT NULL,
        nombre STRING,
        ciudad STRING,
        region STRING,
        creation_date TIMESTAMP,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

dp.create_auto_cdc_flow(
    target="gold.dim_tienda",
    source="silver.tiendas_enriched",
    keys=["id_tienda"],
    sequence_by="_created_at",
    stored_as_scd_type=1,
    name="dim_tienda_cdc_flow"
)
