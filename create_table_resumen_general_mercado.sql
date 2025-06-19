-- =====================================================
-- TABLA: ResumenGeneralMercado
-- ESQUEMA: bolsa_valores
-- DESCRIPCIÓN: Almacena las métricas diarias de operaciones 
--              de los mercados bursátiles (RF y RV, Primario y Secundario)
-- =====================================================

-- Crear la tabla ResumenGeneralMercado
CREATE TABLE bolsa_valores.ResumenGeneralMercado (
    -- Clave primaria y fecha
    id INT IDENTITY(1,1) PRIMARY KEY,
    fecha DATE NOT NULL,
    
    -- ===============================================
    -- MERCADO DE RENTA FIJA - SECUNDARIO
    -- ===============================================
    mdo_secundario_rf_usd DECIMAL(18,2) DEFAULT 0.00,
    mdo_secundario_rf_usd_equiv_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_secundario_rf_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_secundario_rf_total_dop DECIMAL(18,2) DEFAULT 0.00,
    
    -- ===============================================
    -- MERCADO DE RENTA FIJA - PRIMARIO
    -- ===============================================
    mdo_primario_rf_usd DECIMAL(18,2) DEFAULT 0.00,
    mdo_primario_rf_usd_equiv_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_primario_rf_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_primario_rf_total_dop DECIMAL(18,2) DEFAULT 0.00,
    
    -- ===============================================
    -- MERCADO DE RENTA VARIABLE - SECUNDARIO
    -- ===============================================
    mdo_secundario_rv_usd DECIMAL(18,2) DEFAULT 0.00,
    mdo_secundario_rv_usd_equiv_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_secundario_rv_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_secundario_rv_total_dop DECIMAL(18,2) DEFAULT 0.00,
    
    -- ===============================================
    -- MERCADO DE RENTA VARIABLE - PRIMARIO
    -- ===============================================
    mdo_primario_rv_usd DECIMAL(18,2) DEFAULT 0.00,
    mdo_primario_rv_usd_equiv_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_primario_rv_dop DECIMAL(18,2) DEFAULT 0.00,
    mdo_primario_rv_total_dop DECIMAL(18,2) DEFAULT 0.00,
    
    -- ===============================================
    -- CAMPOS DE AUDITORÍA
    -- ===============================================
    fecha_creacion DATETIME DEFAULT GETDATE(),
    fecha_actualizacion DATETIME DEFAULT GETDATE(),
    
    -- ===============================================
    -- CONSTRAINTS
    -- ===============================================
    CONSTRAINT uk_resumen_general_mercado_fecha UNIQUE (fecha)
);

-- =====================================================
-- ÍNDICES PARA OPTIMIZACIÓN
-- =====================================================

-- Índice principal por fecha (para consultas rápidas por fecha)
CREATE INDEX idx_resumen_general_mercado_fecha 
ON bolsa_valores.ResumenGeneralMercado (fecha DESC);

-- Índice para consultas de fecha de creación
CREATE INDEX idx_resumen_general_mercado_fecha_creacion 
ON bolsa_valores.ResumenGeneralMercado (fecha_creacion DESC);

-- =====================================================
-- CONSULTAS DE EJEMPLO Y DESCRIPCIÓN
-- =====================================================

/*
DESCRIPCIÓN DE LA TABLA:
Esta tabla almacena las métricas diarias de operaciones de los mercados bursátiles.
Incluye datos de:
- Renta Fija (RF) y Renta Variable (RV) 
- Mercados Primarios y Secundarios
- 4 tipos de transacciones por mercado: USD, USD Equiv DOP, DOP, Total DOP

ESTRUCTURA DE DATOS:
- 1 registro por fecha
- 16 campos de métricas (4 mercados × 4 tipos de transacción)
- Campos de auditoría para tracking

EJEMPLOS DE CONSULTA:

-- Consultar datos de una fecha específica
SELECT * FROM bolsa_valores.ResumenGeneralMercado 
WHERE fecha = '2025-06-17';

-- Consultar totales por fecha
SELECT 
    fecha,
    (mdo_secundario_rf_usd + mdo_primario_rf_usd + mdo_secundario_rv_usd + mdo_primario_rv_usd) as total_usd,
    (mdo_secundario_rf_total_dop + mdo_primario_rf_total_dop + mdo_secundario_rv_total_dop + mdo_primario_rv_total_dop) as total_dop
FROM bolsa_valores.ResumenGeneralMercado 
ORDER BY fecha DESC;

-- Consultar tendencias de los últimos 30 días
SELECT 
    fecha,
    mdo_secundario_rf_usd,
    mdo_secundario_rf_total_dop,
    mdo_secundario_rv_usd,
    mdo_secundario_rv_total_dop
FROM bolsa_valores.ResumenGeneralMercado 
WHERE fecha >= DATEADD(day, -30, GETDATE())
ORDER BY fecha DESC;

-- Insertar datos de ejemplo
INSERT INTO bolsa_valores.ResumenGeneralMercado (
    fecha,
    mdo_secundario_rf_usd, mdo_secundario_rf_usd_equiv_dop, mdo_secundario_rf_dop, mdo_secundario_rf_total_dop,
    mdo_primario_rf_usd, mdo_primario_rf_usd_equiv_dop, mdo_primario_rf_dop, mdo_primario_rf_total_dop,
    mdo_secundario_rv_usd, mdo_secundario_rv_usd_equiv_dop, mdo_secundario_rv_dop, mdo_secundario_rv_total_dop,
    mdo_primario_rv_usd, mdo_primario_rv_usd_equiv_dop, mdo_primario_rv_dop, mdo_primario_rv_total_dop
) VALUES (
    '2025-06-17',
    3954406.08, 5732587600.20, 5497118140.32, 11100612387.87,  -- Secundario RF
    0.00, 89215369.69, 89215369.69, 89215369.69,                -- Primario RF
    1626923.09, 96888213.48, 11288.47, 103513525.42,            -- Secundario RV
    0.00, 149990545.51, 149990545.51, 149990545.51              -- Primario RV
);
*/ 