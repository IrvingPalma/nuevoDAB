from pyspark.sql import functions as F
from pyspark import pipelines as dp

# ================================================================
# FACT TABLE - Tabla de Hechos
# ================================================================

@dp.materialized_view(
    name="gold.fact_ventas",
    comment="Tabla de hechos de ventas - Usa claves naturales"
)
def fact_ventas():
    """
    Fact table con validación de FKs mediante INNER JOINs.
    Solo incluye ventas con dimensiones válidas.
    """
    # Leer tablas
    ventas = spark.read.table("silver.ventas")
    dim_cliente = spark.read.table("gold.dim_cliente")
    dim_tienda = spark.read.table("gold.dim_tienda")
    dim_producto = spark.read.table("gold.dim_producto")
    dim_tiempo = spark.read.table("gold.dim_tiempo")
    
    # INNER JOINs = validación automática de FKs
    fact = (
        ventas
        # Join con cliente (valida FK)
        .join(
            dim_cliente.select("id_cliente"),
            "id_cliente",
            "inner"  # Solo ventas con cliente válido
        )
        # Join con tienda (valida FK)
        .join(
            dim_tienda.select("id_tienda"),
            "id_tienda",
            "inner"
        )
        # Join con producto (valida FK)
        .join(
            dim_producto.select("id_producto"),
            "id_producto",
            "inner"
        )
        # Join con tiempo (valida FK)
        .join(
            dim_tiempo.select("fecha"),
            F.to_date(F.col("fecha_venta")) == F.col("fecha"),
            "inner"
        )
        .select(
            # Claves foráneas (naturales)
            F.col("id_cliente"),
            F.col("id_tienda"),
            F.col("id_producto"),
            F.to_date(F.col("fecha_venta")).alias("fecha"),
            
            # Clave de negocio
            F.col("id_venta"),
            
            # Métricas
            F.col("cantidad"),
            F.col("monto"),
            F.col("fecha_venta"),
            F.col("anio_mes"),
            
            # Auditoría
            F.current_timestamp().alias("creation_date")
        )
    )
    
    return fact

# ================================================================
# AUDITORÍA - Ventas Rechazadas (VERSIÓN SIMPLIFICADA)
# ================================================================

@dp.materialized_view(
    name="gold.fact_ventas_rechazadas",
    comment="Ventas rechazadas por FK faltantes - Para auditoría"
)
def fact_ventas_rechazadas():
    """
    Identifica ventas que NO tienen FK válidas.
    Usa anti-join para encontrar registros sin match.
    """
    ventas = spark.read.table("silver.ventas")
    dim_cliente = spark.read.table("gold.dim_cliente").select("id_cliente").distinct()
    dim_tienda = spark.read.table("gold.dim_tienda").select("id_tienda").distinct()
    dim_producto = spark.read.table("gold.dim_producto").select("id_producto").distinct()
    dim_tiempo = spark.read.table("gold.dim_tiempo").select("fecha").distinct()
    
    # Crear DataFrames con checks individuales
    ventas_con_fecha = ventas.withColumn("fecha", F.to_date(F.col("fecha_venta")))
    
    # Check clientes - ESTANDARIZAR SELECT
    sin_cliente = (
        ventas_con_fecha
        .join(dim_cliente, "id_cliente", "left_anti")
        .select(
            F.col("id_venta"),
            F.col("id_producto").alias("id_producto_source"),
            F.col("id_cliente").alias("id_cliente_source"),
            F.col("id_tienda").alias("id_tienda_source"),
            F.col("fecha_venta"),
            F.lit("cliente").alias("reason"),
            F.current_timestamp().alias("creation_date")
        )
    )
    
    # Check tiendas - MISMO ORDER DE COLUMNAS
    sin_tienda = (
        ventas_con_fecha
        .join(dim_tienda, "id_tienda", "left_anti")
        .select(
            F.col("id_venta"),
            F.col("id_producto").alias("id_producto_source"),
            F.col("id_cliente").alias("id_cliente_source"),
            F.col("id_tienda").alias("id_tienda_source"),
            F.col("fecha_venta"),
            F.lit("tienda").alias("reason"),
            F.current_timestamp().alias("creation_date")
        )
    )
    
    # Check productos - MISMO ORDER DE COLUMNAS
    sin_producto = (
        ventas_con_fecha
        .join(dim_producto, "id_producto", "left_anti")
        .select(
            F.col("id_venta"),
            F.col("id_producto").alias("id_producto_source"),
            F.col("id_cliente").alias("id_cliente_source"),
            F.col("id_tienda").alias("id_tienda_source"),
            F.col("fecha_venta"),
            F.lit("producto").alias("reason"),
            F.current_timestamp().alias("creation_date")
        )
    )
    
    # Check tiempo - MISMO ORDER DE COLUMNAS
    sin_tiempo = (
        ventas_con_fecha
        .join(dim_tiempo, "fecha", "left_anti")
        .select(
            F.col("id_venta"),
            F.col("id_producto").alias("id_producto_source"),
            F.col("id_cliente").alias("id_cliente_source"),
            F.col("id_tienda").alias("id_tienda_source"),
            F.col("fecha_venta"),
            F.lit("tiempo").alias("reason"),
            F.current_timestamp().alias("creation_date")
        )
    )
    
    # Combinar todos los rechazos con UNION
    rechazados = (
        sin_cliente
        .union(sin_tienda)
        .union(sin_producto)
        .union(sin_tiempo)
        .distinct()  # Eliminar duplicados si un registro falla en múltiples dimensiones
    )
    
    return rechazados
