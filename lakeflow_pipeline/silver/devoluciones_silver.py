# silver/devoluciones_silver.py
"""
Transformación Silver para Devoluciones con AUTO CDC.
Maneja INSERT/UPDATE automáticamente basado en id_devolucion.
"""
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import pipelines as dp
from datetime import datetime

# ================================================================
# FUNCIONES HELPER
# ================================================================

def parse_fecha_devolucion_safe(fecha_col: str):
    """
    Parser robusto de múltiples formatos de fecha para fecha_devolucion.
    """
    col_trimmed = F.trim(F.col(fecha_col))
    
    return F.coalesce(
        # yyyy-MM-dd HH:mm:ss
        F.when(col_trimmed.rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),
               F.to_timestamp(col_trimmed, 'yyyy-MM-dd HH:mm:ss')),
        
        # dd/MM/yyyy HH:mm:ss
        F.when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}$"),
               F.to_timestamp(col_trimmed, 'dd/MM/yyyy HH:mm:ss')),
        
        # MM-dd-yyyy HH:mm:ss
        F.when(col_trimmed.rlike("^\\d{2}-\\d{2}-\\d{4} \\d{2}:\\d{2}:\\d{2}$"),
               F.to_timestamp(col_trimmed, 'MM-dd-yyyy HH:mm:ss')),
        
        # yyyy/MM/dd HH:mm:ss
        F.when(col_trimmed.rlike("^\\d{4}/\\d{2}/\\d{2} \\d{2}:\\d{2}:\\d{2}$"),
               F.to_timestamp(col_trimmed, 'yyyy/MM/dd HH:mm:ss')),
        
        # yyyy-MM-dd
        F.when(col_trimmed.rlike("^\\d{4}-\\d{2}-\\d{2}$"),
               F.to_timestamp(col_trimmed, 'yyyy-MM-dd')),
        
        # dd/MM/yyyy
        F.when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4}$"),
               F.to_timestamp(col_trimmed, 'dd/MM/yyyy')),
        
        # MM/dd/yyyy
        F.when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4}$"),
               F.to_timestamp(col_trimmed, 'MM/dd/yyyy')),
        
        # MM-dd-yyyy
        F.when(col_trimmed.rlike("^\\d{2}-\\d{2}-\\d{4}$"),
               F.to_timestamp(col_trimmed, 'MM-dd-yyyy')),
        
        # dd-MM-yyyy
        F.when(col_trimmed.rlike("^\\d{2}-\\d{2}-\\d{4}$"),
               F.to_timestamp(col_trimmed, 'dd-MM-yyyy')),
        
        F.lit(None).cast(TimestampType())
    )

# ================================================================
# PASO 1: Tabla Bronze limpia
# ================================================================

@dp.table(
    name="bronze.devoluciones_clean",
    comment="Devoluciones limpias listas para CDC",
    table_properties={"quality": "bronze"}
)
@dp.expect_or_drop("valid_id_devolucion", "id_devolucion IS NOT NULL")
def bronze_devoluciones_clean():
    """
    Aplica todas las transformaciones de limpieza.
    AUTO CDC se encarga de la deduplicación.
    """
    df = spark.readStream.table("bronze.devoluciones_raw")
    
    # ============================================================
    # 1. LIMPIEZA DE MOTIVO
    # ============================================================
    # Limpiar valores inválidos y capitalizar primera letra
    df = df.withColumn(
        "motivo_temp",
        F.when(F.col("motivo").isNull(), None)
         .when(F.trim(F.col("motivo")) == "N/A", None)
         .when(F.trim(F.col("motivo")) == "", None)
         .otherwise(F.lower(F.trim(F.col("motivo"))))
    )
    
    # Capitalizar primera letra (ej: "defecto" → "Defecto")
    df = df.withColumn(
        "motivo",
        F.when(F.col("motivo_temp").isNull(), None)
         .otherwise(
             F.concat(
                 F.upper(F.substring(F.col("motivo_temp"), 1, 1)),
                 F.substring(F.col("motivo_temp"), 2, 1000)
             )
         )
    ).drop("motivo_temp")
    
    # ============================================================
    # 2. PARSING DE FECHA_DEVOLUCION (múltiples formatos)
    # ============================================================
    df = df.withColumn(
        "fecha_devolucion",
        parse_fecha_devolucion_safe("fecha_devolucion")
    )
    
    # ============================================================
    # 3. VALIDACIÓN DE RANGO DE FECHAS
    # ============================================================
    fecha_actual = datetime.now().date()
    
    df = df.withColumn(
        "fecha_devolucion",
        F.when(
            (F.col("fecha_devolucion") > F.lit(fecha_actual)) | 
            (F.col("fecha_devolucion") < F.lit("2000-01-01")),
            None
        ).otherwise(F.col("fecha_devolucion"))
    )
    
    # ============================================================
    # 4. CAST FINAL (sin deduplicación - AUTO CDC lo hace)
    # ============================================================
    return df.select(
        F.col("id_devolucion").cast(LongType()),
        F.col("id_venta").cast(LongType()),
        F.col("motivo").cast(StringType()),
        F.col("fecha_devolucion").cast(TimestampType())
    )

# ================================================================
# PASO 2: Tabla Silver destino
# ================================================================

dp.create_streaming_table(
    name="silver.devoluciones",
    comment="Estado actual de devoluciones (SCD Tipo 1)",
    schema="""
        id_devolucion BIGINT NOT NULL,
        id_venta BIGINT,
        motivo STRING,
        fecha_devolucion TIMESTAMP,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

# ================================================================
# PASO 3: AUTO CDC Flow
# ================================================================

dp.create_auto_cdc_flow(
    target="silver.devoluciones",
    source="bronze.devoluciones_clean",
    keys=["id_devolucion"],  # ← PK simple
    sequence_by="fecha_devolucion",  # ← Se queda con el más reciente
    stored_as_scd_type=1,
    name="devoluciones_cdc_flow"
)