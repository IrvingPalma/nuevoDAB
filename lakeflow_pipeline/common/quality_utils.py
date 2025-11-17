# common/quality_utils.py
"""
Constraints de calidad reutilizables
"""

# Constraints para todas las tablas
COMMON_CONSTRAINTS = {
    "valid_id": "expect_or_fail",  # IDs no pueden ser NULL
    "no_negative_numbers": "expect_or_drop",  # Números no negativos
}

# Constraints específicos por entidad
CLIENTES_CONSTRAINTS = {
    "valid_id_cliente": ("id_cliente IS NOT NULL", "fail"),
    "valid_email": ("email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'", "drop"),
}

VENTAS_CONSTRAINTS = {
    "valid_id_venta": ("id_venta IS NOT NULL", "fail"),
    "valid_cantidad": ("cantidad > 0", "drop"),
    "valid_monto": ("monto >= 0", "drop"),
}