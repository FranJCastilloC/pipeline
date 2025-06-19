# Proyecto Economía - Pipeline de Datos

Este proyecto contiene un pipeline de datos para análisis económico que incluye web scraping, limpieza de datos y transformaciones.

## Estructura del Proyecto

- `Run_scraping.py` - Script principal que ejecuta el scraping y procesamiento de datos
- `scraping.py` - Módulo de web scraping
- `transformers/` - Módulo con funciones de limpieza y transformación de datos
  - `ResumenGeneralMercado.py` - Transformaciones específicas para ResumenGeneralMercado
  - `main_limpieza.py` - Funciones generales de limpieza
- `create_table_resumen_general_mercado.sql` - Script SQL para crear tablas
- `Limpieza de datos.ipynb` - Notebook de Jupyter para análisis y limpieza de datos

## Instalación

1. Clona este repositorio
2. Instala las dependencias:
   ```
   pip install pandas numpy
   ```

## Uso

Para ejecutar el pipeline completo:

```python
python Run_scraping.py
```

El script:
1. Ejecuta el scraping de datos desde una fecha específica hasta hoy
2. Organiza los datos por tipo de reporte
3. Aplica limpieza automática usando las funciones correspondientes
4. Genera DataFrames limpios listos para análisis

## Características

- **Scraping automatizado**: Extrae datos de múltiples fechas
- **Limpieza modular**: Sistema extensible de funciones de limpieza por tipo de datos
- **Organización automática**: Agrupa datos por tipo de reporte
- **Variables globales**: Crea automáticamente variables para cada DataFrame limpio

## Contribuir

Este proyecto está en desarrollo activo. Para contribuir, por favor crea un fork y envía pull requests. 