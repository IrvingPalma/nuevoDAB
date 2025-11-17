from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark import pipelines as dp
from datetime import datetime

# ================================================================
# FUNCIONES HELPER
# ================================================================

def parse_fecha_registro_safe(fecha_col: str):
    """
    Parser robusto de múltiples formatos de fecha para fecha_registro.
    """
    col_trimmed = F.trim(F.col(fecha_col))
    
    return F.coalesce(
        # yyyy-MM-dd
        F.when(col_trimmed.rlike("^\\d{4}-\\d{2}-\\d{2}$"),
               F.to_date(col_trimmed, 'yyyy-MM-dd')),
        
        # dd/MM/yyyy
        F.when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4}$"),
               F.to_date(col_trimmed, 'dd/MM/yyyy')),
        
        # MM-dd-yyyy
        F.when(col_trimmed.rlike("^\\d{2}-\\d{2}-\\d{4}$"),
               F.to_date(col_trimmed, 'MM-dd-yyyy')),
        
        # dd-MM-yyyy
        F.when(col_trimmed.rlike("^\\d{2}-\\d{2}-\\d{4}$"),
               F.to_date(col_trimmed, 'dd-MM-yyyy')),
        
        # yyyy/MM/dd (con o sin timestamp)
        F.when(col_trimmed.rlike("^\\d{4}/\\d{2}/\\d{2}"),
               F.to_date(F.substring(col_trimmed, 1, 10), 'yyyy/MM/dd')),
        
        # yyyy-MM-dd HH:mm:ss (extraer solo la fecha)
        F.when(col_trimmed.rlike("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"),
               F.to_date(F.substring(col_trimmed, 1, 10), 'yyyy-MM-dd')),
        
        # dd/MM/yyyy HH:mm:ss (extraer solo la fecha)
        F.when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}$"),
               F.to_date(F.substring(col_trimmed, 1, 10), 'dd/MM/yyyy')),
        
        # MM/dd/yyyy
        F.when(col_trimmed.rlike("^\\d{2}/\\d{2}/\\d{4}$"),
               F.to_date(col_trimmed, 'MM/dd/yyyy')),
        
        F.lit(None).cast(DateType())
    )

# ================================================================
# PASO 1: Tabla Bronze limpia CON WARNINGS (NO bloquea)
# ================================================================

@dp.table(
    name="bronze.clientes_clean",
    comment="Clientes limpios con validaciones registradas",
    table_properties={"quality": "bronze"}
)
# ⚠️ WARNINGS - Se registran en event log pero NO bloquean el flujo
@dp.expect("warning_id_cliente_null", "id_cliente IS NOT NULL")
@dp.expect("warning_email_null", "email IS NOT NULL")
@dp.expect("warning_email_format", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'")
@dp.expect("warning_fecha_registro_null", "fecha_registro IS NOT NULL")
@dp.expect("warning_fecha_registro_valida", "fecha_registro >= '1900-01-01' AND fecha_registro <= current_date()")
def bronze_clientes_clean():
    """
    Aplica todas las transformaciones de limpieza.
    Los @dp.expect() registran warnings pero NO bloquean.
    """
    df = spark.readStream.table("bronze.clientes_raw")
    
    # ============================================================
    # 1. LIMPIEZA DE NOMBRE
    # ============================================================
    df = df.withColumn(
        "nombre",
        F.initcap(F.trim(F.col("nombre")))
    )
    
    # ============================================================
    # 2. LIMPIEZA DE CIUDAD
    # ============================================================
    df = df.withColumn(
        "ciudad",
        F.when(F.col("ciudad").isNull(), None)
         .otherwise(F.initcap(F.trim(F.col("ciudad"))))
    )
    
    # ============================================================
    # 3. LIMPIEZA Y VALIDACIÓN DE EMAIL
    # ============================================================
    # Limpiar nulls y "null" strings
    df = df.withColumn(
        "email",
        F.when(F.col("email").isNull(), None)
         .when(F.lower(F.trim(F.col("email"))) == "null", None)
         .otherwise(F.lower(F.trim(F.col("email"))))
    )
    
    # Validar formato de email con regex
    email_pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    df = df.withColumn(
        "email",
        F.when(F.col("email").rlike(email_pattern), F.col("email"))
         .otherwise(None)
    )
    
    # ============================================================
    # 4. PARSING DE FECHA_REGISTRO (múltiples formatos)
    # ============================================================
    df = df.withColumn(
        "fecha_registro",
        parse_fecha_registro_safe("fecha_registro")
    )
    
    # ============================================================
    # 5. VALIDACIÓN DE RANGO DE FECHAS
    # ============================================================
    fecha_actual = datetime.now().date()
    
    df = df.withColumn(
        "fecha_registro",
        F.when(
            (F.col("fecha_registro") > F.lit(fecha_actual)) | 
            (F.col("fecha_registro") < F.lit("1900-01-01")),
            None
        ).otherwise(F.col("fecha_registro"))
    )
    
    # ============================================================
    # 6. CAST FINAL
    # ============================================================
    return df.select(
        F.col("id_cliente").cast(LongType()),
        F.col("nombre").cast(StringType()),
        F.col("email").cast(StringType()),
        F.col("ciudad").cast(StringType()),
        F.col("fecha_registro").cast(DateType())
    )

# ================================================================
# PASO 2: SEPARACIÓN VÁLIDOS vs CUARENTENA
# ================================================================

@dp.table(
    name="silver.clientes_validos_temp",
    comment="Clientes que pasan TODAS las validaciones - temporal para CDC",
    table_properties={"quality": "silver"}
)
def silver_clientes_validos_temp():
    """
    Solo clientes válidos - estos irán a la tabla final con AUTO CDC.
    """
    df = spark.readStream.table("bronze.clientes_clean")
    
    # Filtrar SOLO los que pasan TODAS las validaciones
    df_validos = df.filter(
        (F.col("id_cliente").isNotNull()) &
        (F.col("email").isNotNull()) &
        (F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) &
        (F.col("fecha_registro").isNotNull()) &
        (F.col("fecha_registro") >= F.lit("1900-01-01")) &
        (F.col("fecha_registro") <= F.current_date())
    )
    
    return df_validos

@dp.table(
    name="silver.clientes_cuarentena",
    comment="Clientes rechazados - requieren revisión manual",
    table_properties={"quality": "silver"}
)
def silver_clientes_cuarentena():
    """
    Registros que fallaron AL MENOS UNA validación.
    Se guardan con razones detalladas para análisis.
    """
    df = spark.readStream.table("bronze.clientes_clean")
    
    # Filtrar SOLO los INVÁLIDOS (al menos una validación falla)
    df_invalidos = df.filter(
        (F.col("id_cliente").isNull()) |
        (F.col("email").isNull()) |
        (~F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")) |
        (F.col("fecha_registro").isNull()) |
        (F.col("fecha_registro") < F.lit("1900-01-01")) |
        (F.col("fecha_registro") > F.current_date())
    )
    
    # Agregar columnas con razones detalladas de rechazo
    df_con_razones = (
        df_invalidos
        # Identificar cada tipo de problema
        .withColumn("rechazo_id_null",
            F.when(F.col("id_cliente").isNull(), "ID cliente faltante").otherwise(""))
        .withColumn("rechazo_email_null",
            F.when(F.col("email").isNull(), "Email faltante").otherwise(""))
        .withColumn("rechazo_email_formato",
            F.when(
                (F.col("email").isNotNull()) &
                (~F.col("email").rlike("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")),
                "Email formato inválido"
            ).otherwise(""))
        .withColumn("rechazo_fecha_null",
            F.when(F.col("fecha_registro").isNull(), "Fecha registro faltante").otherwise(""))
        .withColumn("rechazo_fecha_rango",
            F.when(
                (F.col("fecha_registro").isNotNull()) &
                ((F.col("fecha_registro") < F.lit("1900-01-01")) |
                 (F.col("fecha_registro") > F.current_date())),
                "Fecha registro fuera de rango"
            ).otherwise(""))
        
        # Concatenar todas las razones (eliminar strings vacíos)
        .withColumn("razones_rechazo",
            F.concat_ws(", ",
                F.col("rechazo_id_null"),
                F.col("rechazo_email_null"),
                F.col("rechazo_email_formato"),
                F.col("rechazo_fecha_null"),
                F.col("rechazo_fecha_rango")
            ))
        
        # Metadata de cuarentena
        .withColumn("fecha_cuarentena", F.current_timestamp())
        .withColumn("estado", F.lit("PENDIENTE_REVISION"))
    )
    
    return df_con_razones.select(
        "id_cliente",
        "nombre",
        "email",
        "ciudad",
        "fecha_registro",
        "razones_rechazo",
        "fecha_cuarentena",
        "estado"
    )

# ================================================================
# PASO 3: Tabla Silver FINAL con AUTO CDC (SOLO VÁLIDOS)
# ================================================================

dp.create_streaming_table(
    name="silver.clientes",
    comment="Estado actual de clientes VÁLIDOS (SCD Tipo 1)",
    schema="""
        id_cliente BIGINT NOT NULL,
        nombre STRING,
        email STRING,
        ciudad STRING,
        fecha_registro DATE,
        _created_at TIMESTAMP,
        _last_updated_at TIMESTAMP
    """
)

# ================================================================
# PASO 4: AUTO CDC Flow (SOLO para registros válidos)
# ================================================================

dp.create_auto_cdc_flow(
    target="silver.clientes",
    source="silver.clientes_validos_temp",
    keys=["id_cliente"],
    sequence_by="fecha_registro",
    stored_as_scd_type=1,
    name="clientes_cdc_flow"
)