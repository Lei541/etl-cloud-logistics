-- =====================================================
-- FLEETLOGIX - DATA WAREHOUSE DIMENSIONAL MODEL
-- Modelo estrella para análisis en Snowflake
-- =====================================================

-- Crear warehouse y database en Snowflake
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS FLEETLOGIX_WH3 WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60;
CREATE DATABASE IF NOT EXISTS FLEETLOGIX_DW3;
USE DATABASE FLEETLOGIX_DW3;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- =====================================================
-- DIMENSIONES
-- =====================================================

-- Dimensión Fecha
CREATE OR REPLACE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_num INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name VARCHAR(50),
    fiscal_quarter INT,
    fiscal_year INT
);

-- Dimensión Tiempo (para análisis por hora)
CREATE OR REPLACE TABLE dim_time (
    time_key INT PRIMARY KEY,
    hour INT,
    minute INT,
    second INT,
    time_of_day VARCHAR(20), -- 'Madrugada', 'Mañana', 'Tarde', 'Noche'
    hour_24 VARCHAR(5),       -- '14:30'
    hour_12 VARCHAR(8),       -- '02:30 PM'
    am_pm VARCHAR(2),
    is_business_hour BOOLEAN,
    shift VARCHAR(20)         -- 'Turno 1', 'Turno 2', 'Turno 3'
);

-- Dimensión Vehículo
CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_key INT PRIMARY KEY,
    vehicle_id INT NOT NULL,
    license_plate VARCHAR(20),
    vehicle_type VARCHAR(50),
    capacity_kg DECIMAL(10,2),
    fuel_type VARCHAR(20),
    acquisition_date DATE,
    age_months INT,
    status VARCHAR(20),
    last_maintenance_date DATE,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

-- Dimensión Conductor
CREATE OR REPLACE TABLE dim_driver (
    driver_key INT PRIMARY KEY,
    driver_id INT NOT NULL,
    employee_code VARCHAR(20),
    full_name VARCHAR(200),
    license_number VARCHAR(50),
    license_expiry DATE,
    phone VARCHAR(20),
    hire_date DATE,
    experience_months INT,
    status VARCHAR(20),
    performance_category VARCHAR(20), -- 'Alto', 'Medio', 'Bajo'
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

-- Dimensión Ruta
CREATE OR REPLACE TABLE dim_route (
    route_key INT PRIMARY KEY,
    route_id INT NOT NULL,
    route_code VARCHAR(20),
    origin_city VARCHAR(100),
    destination_city VARCHAR(100),
    distance_km DECIMAL(10,2),
    estimated_duration_hours DECIMAL(5,2),
    toll_cost DECIMAL(10,2),
    difficulty_level VARCHAR(20), -- 'Fácil', 'Medio', 'Difícil'
    route_type VARCHAR(20)        -- 'Urbana', 'Interurbana', 'Rural'
);

-- Dimensión Cliente
CREATE OR REPLACE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id INT IDENTITY,
    customer_name VARCHAR(200),
    customer_type VARCHAR(50),    -- 'Individual', 'Empresa', 'Gobierno'
    city VARCHAR(100),
    first_delivery_date DATE,
    total_deliveries INT,
    customer_category VARCHAR(20)  -- 'Premium', 'Regular', 'Ocasional'
);

-- =====================================================
-- TABLA DE HECHOS
-- =====================================================

CREATE OR REPLACE TABLE fact_deliveries (
    -- Keys
    delivery_key INT IDENTITY PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    scheduled_time_key INT REFERENCES dim_time(time_key),
    delivered_time_key INT REFERENCES dim_time(time_key),
    vehicle_key INT REFERENCES dim_vehicle(vehicle_key),
    driver_key INT REFERENCES dim_driver(driver_key),
    route_key INT REFERENCES dim_route(route_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    
    -- Degenerate dimensions
    delivery_id INT NOT NULL,
    trip_id INT NOT NULL,
    tracking_number VARCHAR(50),
    
    -- Métricas
    package_weight_kg DECIMAL(10,2),
    distance_km DECIMAL(10,2),
    fuel_consumed_liters DECIMAL(10,2),
    delivery_time_minutes INT,
    delay_minutes INT,
    
    -- Métricas calculadas
    deliveries_per_hour DECIMAL(5,2),
    fuel_efficiency_km_per_liter DECIMAL(5,2),
    cost_per_delivery DECIMAL(10,2),
    revenue_per_delivery DECIMAL(10,2),
    
    -- Indicadores
    is_on_time BOOLEAN,
    is_damaged BOOLEAN,
    has_signature BOOLEAN,
    delivery_status VARCHAR(20),
    
    -- Auditoría
    etl_batch_id INT,
    etl_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- CONFIGURACIÓN SNOWFLAKE
-- =====================================================

-- Habilitar Time Travel (30 días)
ALTER TABLE fact_deliveries SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_date SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_vehicle SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_driver SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_route SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_customer SET DATA_RETENTION_TIME_IN_DAYS = 30;
ALTER TABLE dim_time SET DATA_RETENTION_TIME_IN_DAYS = 30;

-- Crear tabla de staging para ETL
CREATE OR REPLACE TABLE staging_daily_load (
    raw_data VARIANT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Tablas agregadas con analítica: 

CREATE TABLE IF NOT EXISTS ANALYTICS.REPORT_DRIVER_TOTALS (
  DRIVER_KEY NUMBER,
  TOTAL_DELIVERIES NUMBER,
  AVG_DELAY_MINUTES NUMBER(10,2),
  TOTAL_REVENUE NUMBER(18,2)
);

CREATE TABLE IF NOT EXISTS ANALYTICS.REPORT_ROUTE_TOTALS (
  ROUTE_KEY NUMBER,
  TOTAL_DELIVERIES NUMBER,
  AVG_DISTANCE_KM NUMBER(10,2),
  TOTAL_REVENUE NUMBER(18,2)
);
-- =====================================================
-- VISTAS SEGURAS POR ROL
-- =====================================================

-- Vista para Ventas (solo sus clientes)
CREATE OR REPLACE SECURE VIEW v_sales_deliveries AS
SELECT 
    d.full_date,
    c.customer_name,
    c.customer_type,
    f.package_weight_kg,
    f.delivery_status,
    f.revenue_per_delivery
FROM fact_deliveries f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.customer_type != 'Individual'; -- Restricción ejemplo

-- Vista para Operaciones (todo)
CREATE OR REPLACE SECURE VIEW v_operations_deliveries AS
SELECT 
    d.full_date,
    t.hour_24 as hora,
    v.license_plate,
    dr.full_name as conductor,
    r.route_code,
    c.customer_name,
    f.delivery_time_minutes,
    f.delay_minutes,
    f.is_on_time,
    f.fuel_consumed_liters
FROM fact_deliveries f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_time t ON f.scheduled_time_key = t.time_key
JOIN dim_vehicle v ON f.vehicle_key = v.vehicle_key
JOIN dim_driver dr ON f.driver_key = dr.driver_key
JOIN dim_route r ON f.route_key = r.route_key
JOIN dim_customer c ON f.customer_key = c.customer_key;

-- Crear roles
CREATE ROLE IF NOT EXISTS SALES_ANALYST;
CREATE ROLE IF NOT EXISTS OPERATIONS_ANALYST;

-- Asignar permisos
GRANT SELECT ON VIEW v_sales_deliveries TO ROLE SALES_ANALYST;
GRANT SELECT ON VIEW v_operations_deliveries TO ROLE OPERATIONS_ANALYST;