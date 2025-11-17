# silver/ventas_silver_with_cdc.py
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark import pipelines as dp
from datetime import datetime

# ================================================================
# PASO 1: Tabla Bronze limpia (con todas las transformaciones)
# ================================================================

@dp.table(
    name="bronze.ventas_clean",
    comment="Ventas limpias listas para CDC",
    table_properties={"quality": "bronze"}
)
@dp.expect_or_drop("valid_id_venta", "id_venta IS NOT NULL")
@dp.expect_or_drop("valid_id_producto", "id_producto IS NOT NULL")
def bronze_ventas_clean():
    """
    Aplica TODAS las transformaciones de limpieza aquí.
    Esta es la versión "limpia" lista para CDC.
    """
    df = spark.readStream.table("bronze.ventas_raw")
    
    # (Tu lógica compleja de limpieza aquí)
    # Mapeo de cantidades literales
    numeros_literales = {
        "cero": "0", "uno": "1", "dos": "2", "tres": "3",
        "cuatro": "4", "cinco": "5", "seis": "6", "siete": "7",
        "ocho": "8", "nueve": "9", "diez": "10"
    }
    from itertools import chain
    mapping_numeros = F.create_map([F.lit(x) for x in chain(*numeros_literales.items())])
    
    df = df.withColumn(
        "cantidad_mapeada",
        F.coalesce(
            mapping_numeros[F.lower(F.trim(F.col("cantidad")))],
            F.col("cantidad")
        )
    )
    
    df = df.withColumn(
        "cantidad",
        F.when(
            F.expr("try_cast(cantidad_mapeada as INT)") > 0,
            F.expr("try_cast(cantidad_mapeada as INT)")
        ).otherwise(None)
    ).drop("cantidad_mapeada")
    
    # Limpieza de monto
    df = df.withColumn(
        "monto",
        F.when(
            F.expr("try_cast(monto as DOUBLE)") > 0,
            F.round(F.expr("try_cast(monto as DOUBLE)"), 2)
        ).otherwise(None)
    )
    
    # Parsing de fechas (simplificado)
    df = df.withColumn(
        "fecha_venta",
        F.coalesce(
            F.to_timestamp(F.trim(F.col("fecha_venta")), 'yyyy/MM/dd HH:mm:ss'),
            F.to_timestamp(F.trim(F.col("fecha_venta")), 'yyyy-MM-dd HH:mm:ss'),
            F.to_timestamp(F.trim(F.col("fecha_venta")), 'dd/MM/yyyy HH:mm:ss')
        )
    )
    
    df = df.withColumn(
        "updated_at",
        F.coalesce(
            F.to_timestamp(F.trim(F.col("updated_at")), 'yyyy/MM/dd HH:mm:ss'),
            F.to_timestamp(F.trim(F.col("updated_at")), 'yyyy-MM-dd HH:mm:ss'),
            F.to_timestamp(F.trim(F.col("updated_at")), 'dd/MM/yyyy HH:mm:ss')
        )
    )
    
    # Validación de rangos
    fecha_actual = datetime.now()
    df = df.withColumn(
        "fecha_venta",
        F.when(
            (F.col("fecha_venta") > F.lit(fecha_actual)) | 
            (F.col("fecha_venta") < F.lit("2000-01-01")),
            None
        ).otherwise(F.col("fecha_venta"))
    )
    
    # Columna derivada
    df = df.withColumn(
        "anio_mes",
        F.when(F.col("fecha_venta").isNotNull(), 
               F.date_format(F.col("fecha_venta"), "yyyy-MM"))
    )
    
    # Cast final
    return df.select(
        F.col("id_venta").cast(LongType()),
        F.col("id_cliente").cast(LongType()),
        F.col("id_tienda").cast(LongType()),
        F.col("id_producto").cast(LongType()),
        F.col("cantidad").cast(IntegerType()),
        F.col("monto").cast(DecimalType(18, 2)),
        F.col("fecha_venta").cast(TimestampType()),
        F.col("updated_at").cast(TimestampType()),
        F.col("anio_mes").cast(StringType())
    )

# ================================================================
# PASO 2: Tabla Silver destino (vacía)
# ================================================================

dp.create_streaming_table(
    name="silver.ventas",
    comment="Estado actual de ventas (SCD Tipo 1)",
    schema="""
        id_venta BIGINT NOT NULL,
        id_cliente BIGINT,
        id_tienda BIGINT,
        id_producto BIGINT NOT NULL,
        cantidad INT,
        monto DECIMAL(18,2),
        fecha_venta TIMESTAMP,
        updated_at TIMESTAMP,
        anio_mes STRING,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

# ================================================================
# PASO 3: Flujo AUTO CDC (maneja INSERT/UPDATE automáticamente)
# ================================================================

dp.create_auto_cdc_flow(
    target="silver.ventas",
    source="bronze.ventas_clean",
    keys=["id_venta", "id_producto"],  # ← PK compuesta
    sequence_by="updated_at",  # ← Para resolver conflictos out-of-order
    stored_as_scd_type=1,  # ← SCD Tipo 1 (sobrescribe)
    name="ventas_cdc_flow"
)