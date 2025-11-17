from pyspark.sql import functions as F
from pyspark import pipelines as dp


# ================================================================
# DIMENSIÓN CLIENTE (sin surrogate key)
# ================================================================

@dp.table(
    name="silver.clientes_enriched",
    comment="Clientes preparados para Gold",
    table_properties={"quality": "silver"}
)
def silver_clientes_enriched():
    """Agregar columnas adicionales para Gold"""
    return (
        spark.readStream.table("silver.clientes")
        .select(
            "id_cliente",
            "nombre",
            "email",
            "ciudad",
            "fecha_registro",
            "_created_at",
            "_last_updated_at"
        )
        .withColumn("creation_date", F.current_timestamp())
    )

# Dimensión Cliente
dp.create_streaming_table(
    name="gold.dim_cliente",
    comment="Dimensión Cliente - Clave natural: id_cliente",
    schema="""
        id_cliente BIGINT NOT NULL,
        nombre STRING,
        email STRING,
        ciudad STRING,
        fecha_registro DATE,
        creation_date TIMESTAMP,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

dp.create_auto_cdc_flow(
    target="gold.dim_cliente",
    source="silver.clientes_enriched",
    keys=["id_cliente"],  # ← Clave natural como PK
    sequence_by="fecha_registro",
    stored_as_scd_type=1,
    name="dim_cliente_cdc_flow"
)



