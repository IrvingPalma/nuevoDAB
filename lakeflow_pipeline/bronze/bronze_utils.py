# bronze/bronze_utils.py
"""
Utilidades para ingesta Bronze metadata-driven
"""
import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_audit_columns(df: DataFrame) -> DataFrame:
    """Agrega columnas de auditoría estándar"""
    return (
        df
        .withColumn("_src_file", F.col("_metadata.file_path"))
        .withColumn("_created_at", F.current_timestamp())
    )

def align_to_target_schema(df: DataFrame, target_table: str, spark) -> DataFrame:
    """
    Alinea el DataFrame a las columnas de la tabla destino.
    Agrega columnas faltantes como NULL y castea todo a STRING.
    """
    target_cols = [f.name for f in spark.table(target_table).schema]
    
    # Agregar columnas faltantes
    for col in target_cols:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))
    
    # Seleccionar solo columnas del target y castear a STRING
    return df.select([F.col(c).cast("string") for c in target_cols])