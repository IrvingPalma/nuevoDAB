from pyspark.sql.types import *

# Schema para clientes
""""
CLIENTES_SCHEMA = StructType([
    StructField("id_cliente", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("email", StringType(), True),
    StructField("ciudad", StringType(), True),
    StructField("fecha_registro", StringType(), True)
])
"""
def create_schema():
    schema = """
            id_cliente INT,
            nombre STRING,
            email STRING,
            ciudad STRING,
            fecha_registro STRING
           """
    return schema
