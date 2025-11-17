from pyspark.sql import functions as F
from pyspark import pipelines as dp


# ================================================================
# DIMENSIÓN TIEMPO (sin surrogate key)
# ================================================================

@dp.table(
    name="silver.tiempo_derivado",
    comment="Dimensión tiempo derivada desde ventas"
)
def silver_tiempo_derivado():
    """
    Genera fechas únicas desde ventas.
    Deduplicación automática con AUTO CDC.
    """
    return (
        spark.readStream.table("silver.ventas")
        .select(F.to_date("fecha_venta").alias("fecha"))
        .where("fecha IS NOT NULL")
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("dia", F.dayofmonth("fecha"))
        .withColumn("anio_mes", F.date_format("fecha", "yyyy-MM"))
        .withColumn("nombre_mes", F.date_format("fecha", "MMMM"))
        .withColumn("dia_semana", F.dayofweek("fecha"))
        .withColumn("nombre_dia", F.date_format("fecha", "EEEE"))
        .withColumn("trimestre", F.quarter("fecha"))
        .withColumn("creation_date", F.current_timestamp())
    )

# Dimensión Tiempo
dp.create_streaming_table(
    name="gold.dim_tiempo",
    comment="Dimensión Tiempo - Clave natural: fecha",
    schema="""
        fecha DATE NOT NULL,
        anio INT,
        mes INT,
        dia INT,
        anio_mes STRING,
        nombre_mes STRING,
        dia_semana INT,
        nombre_dia STRING,
        trimestre INT,
        creation_date TIMESTAMP,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

dp.create_auto_cdc_flow(
    target="gold.dim_tiempo",
    source="silver.tiempo_derivado",
    keys=["fecha"],  # ← Fecha como PK natural
    sequence_by="creation_date",
    stored_as_scd_type=1,
    name="dim_tiempo_cdc_flow"
)