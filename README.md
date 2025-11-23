# ETL, Data Warehouse y Arquitectura Serverless (Proyecto Integrador II)

[![Python](https://img.shields.io/badge/python-3.x-blue)]()
[![License: MIT](https://img.shields.io/badge/license-MIT-green)]()

## Resumen
**FleetLogix Data Architecture** es un proyecto integrador que diseña e implementa una solución completa de datos para una empresa de logística (≈200 vehículos, 5 ciudades), abarcando generación de datos, base transaccional, análisis SQL, Data Warehouse y arquitectura en la nube.  

Incluye:
- Generación masiva de datos sintéticos (≈500k registros).
- Base transaccional en PostgreSQL con validaciones.
- Consultas SQL analíticas optimizadas.
- Data Warehouse en Snowflake con modelo en estrella y SCD Type 2.
- Pipeline ETL en Python para cargas incrementales.
- Arquitectura serverless en AWS para ingesta y procesamiento en tiempo real.

---

## Estructura del repositorio
FleetLogix_PI2/

├── data_generation/
│ ├── generate_data.py # Script para generar datos sintéticos (Faker + pandas)
│ └── logs/ # Logs de ejecución y control de calidad
│
├── sql_analysis/
│ ├── queries.sql # Consultas SQL analíticas (12 queries clave)
│ ├── explain_analyze_results.txt
│ └── performance_report.docx # Informe técnico de optimización
│
├── warehouse_snowflake/
│ ├── ddl_snowflake.sql # DDL - tablas del DW (hechos + dimensiones)
│ ├── etl_pipeline.py # Pipeline ETL para carga incremental y SCD2
│ └── config_scd2_notes.md
│
├── cloud_architecture/
│ ├── aws_diagram.png # Diagrama de arquitectura (AWS serverless)
│ ├── lambda_functions/ # Código de Lambdas (Python)
│ └── boto3_scripts.py # Scripts auxiliares para S3/RDS/DynamoDB
│
└── README.md


---

## Contenido del proyecto

### **AVANCE 1 — Generación y carga de datos sintéticos**
- `generate_data.py` genera ~500k registros distribuidos entre:
  - `vehicles` (200)
  - `drivers` (400)
  - `routes` (50)
  - `trips` (100k)
  - `deliveries` (400k)
  - `maintenance` (5k)
- Validaciones implementadas:
  - Integridad referencial.
  - Consistencia temporal (`arrival > departure`).
  - Logs de carga y control de errores.

---

### **AVANCE 2 — Consultas SQL y análisis de rendimiento**
- 12 consultas analíticas para KPIs operativos:
  - Eficiencia de rutas.
  - Consumo de combustible.
  - Desempeño de conductores.
  - Puntualidad de entregas.
  - Mantenimiento preventivo.
- Optimización con:
  - `EXPLAIN ANALYZE`
  - Índices en columnas críticas.
- Reducción promedio del tiempo de ejecución: **~13%**.

---

### **AVANCE 3 — Data Warehouse y pipeline ETL**
- Modelo estrella en Snowflake:
  - **Hechos:** `fact_deliveries`
  - **Dimensiones:** `dim_date`, `dim_time`, `dim_vehicle`, `dim_driver`, `dim_route`, `dim_customer`
- Pipeline ETL en Python:
  - Extracción desde PostgreSQL.
  - Transformaciones con pandas.
  - Historización con **SCD Type 2**.
  - Carga incremental.
  - Logs de auditoría.
- Preparado para automatización diaria.

---

### **AVANCE 4 — Arquitectura cloud en AWS**
- Componentes principales:
  - **API Gateway** — punto de entrada de datos de flota.
  - **AWS Lambda** — procesamiento, validación y alertas.
  - **Amazon S3** — almacenamiento por particiones de fecha.
  - **AWS RDS (PostgreSQL)** — base transaccional.
  - **DynamoDB** — estado en tiempo real de entregas.
- Funciones Lambda:
  - Verificación de finalización de entregas.
  - Cálculo de ETA.
  - Alertas automáticas por desvíos.

---

## Tecnologías utilizadas

**Lenguaje**  
- Python 3.x

**Bases de datos**  
- PostgreSQL  
- Snowflake  
- DynamoDB

**Cloud**  
- AWS: S3, Lambda, API Gateway, RDS, DynamoDB  

**ETL / Automatización**  
- pandas  
- Faker  
- SQLAlchemy  
- boto3  
- schedule  

**Analítica SQL**  
- PostgreSQL (`EXPLAIN ANALYZE`, índices, ventanas)

---
